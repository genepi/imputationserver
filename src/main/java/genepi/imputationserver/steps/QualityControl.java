package genepi.imputationserver.steps;

import genepi.hadoop.HdfsUtil;
import genepi.hadoop.common.ContextLog;
import genepi.hadoop.common.WorkflowContext;
import genepi.hadoop.io.HdfsLineWriter;
import genepi.imputationserver.steps.qc.QualityControlJob;
import genepi.imputationserver.steps.vcf.VcfFile;
import genepi.imputationserver.steps.vcf.VcfFileUtil;
import genepi.imputationserver.util.HadoopJobStep;
import genepi.imputationserver.util.RefPanel;
import genepi.imputationserver.util.RefPanelList;
import genepi.io.FileUtil;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.List;
import java.util.Vector;

import cloudgene.mapred.jobs.Message;

public class QualityControl extends HadoopJobStep {

	private DecimalFormat formatter = new DecimalFormat("###,###.###");

	@Override
	public boolean run(WorkflowContext context) {

		String folder = getFolder(HadoopJobStep.class);

		// inputs
		String reference = context.get("refpanel");
		String population = context.get("population");
		String files = context.get("files");
		String chunkfile = context.get("chunkfile");

		// outputs
		String output = context.get("outputmaf");
		String outputManifest = context.get("mafchunkfile");
		String removedSnps = context.get("statistics");

		// read config
		// PreferenceStore store = new PreferenceStore(new File(FileUtil.path(
		// folder, "job.config")));
		// int chunkSize =
		// Integer.parseInt(store.getString("pipeline.chunksize"));

		int chunkSize = Integer.parseInt(context.get("chunksize"));

		int chunks = 0;
		// create manifest file
		try {
			chunks = createChunkFile(context, files, chunkfile, chunkSize);
		} catch (Exception e) {
			e.printStackTrace();
			context.error(e.toString());
			return false;
		}

		if (chunks == -1) {
			context.error("Error during manifest file creation.");
			return false;
		}

		// load reference panels
		RefPanelList panels = null;
		try {
			panels = RefPanelList.loadFromFile(FileUtil.path(folder,
					"panels.txt"));

		} catch (Exception e) {

			context.error("panels.txt not found.");
			return false;
		}

		// check reference panel
		RefPanel panel = panels.getById(reference);
		if (panel == null) {
			context.error("Reference '" + reference + "' not found.");
			context.error("Available references:");
			for (RefPanel p : panels.getPanels()) {
				context.error(p.getId());
			}

			return false;
		}

		// submit qc hadoop job
		QualityControlJob job = new QualityControlJob(context.getJobId()
				+ "-quality-control", new ContextLog(context));
		job.setPanelId(panel.getId());
		job.setLegendHdfs(panel.getLegend());
		job.setLegendPattern(panel.getLegendPattern());
		job.setPopulation(population);
		job.setInput(chunkfile);
		job.setOutput(output + "_temp");
		job.setOutputMaf(output);
		job.setOutputManifest(outputManifest);
		job.setOutputRemovedSnps(removedSnps);
		job.setJarByClass(QualityControl.class);

		boolean successful = executeHadoopJob(job, context);

		// job.downloadLogs("/home/lukas/mylog");

		if (successful) {

			// print qc statistics
			DecimalFormat df = new DecimalFormat("#.00");

			StringBuffer text = new StringBuffer();

			text.append("<b>Statistics:</b> <br>");
			text.append("Alternative allele frequency > 0.5 sites: "
					+ formatter.format(job.getAlternativeAlleles()) + "<br>");
			text.append("Reference Overlap: "
					+ df.format(job.getFoundInLegend()
							/ (double) (job.getFoundInLegend() + job
									.getNotFoundInLegend()) * 100) + "% "
					+ "<br>");

			text.append("Match: " + formatter.format(job.getMatch()) + "<br>");
			text.append("Allele switch: "
					+ formatter.format(job.getAlleleSwitch()) + "<br>");
			text.append("Strand flip: "
					+ formatter.format(job.getStrandSwitch1()) + "<br>");
			text.append("Strand flip and allele switch: "
					+ formatter.format(job.getStrandSwitch3()) + "<br>");
			text.append("A/T, C/G genotypes: "
					+ formatter.format(job.getStrandSwitch2()) + "<br>");

			text.append("<b>Filtered sites:</b> <br>");
			text.append("Filter flag set: "
					+ formatter.format(job.getFilterFlag()) + "<br>");
			text.append("Invalid alleles: "
					+ formatter.format(job.getInvalidAlleles()) + "<br>");
			text.append("Duplicated sites: "
					+ formatter.format(job.getDuplicates()) + "<br>");
			text.append("NonSNP sites: " + formatter.format(job.getNoSnps())
					+ "<br>");
			text.append("Monomorphic sites: "
					+ formatter.format(job.getMonomorphic()) + "<br>");
			text.append("Allele mismatch: "
					+ formatter.format(job.getAlleleMismatch()) + "<br>");
			text.append("SNPs call rate < 90%: "
					+ formatter.format(job.getToLessSamples()));

			context.ok(text.toString());

			text = new StringBuffer();

			text.append("Excluded sites in total: "
					+ formatter.format(job.getFiltered()) + "<br>");
			text.append("Remaining sites in total: "
					+ formatter.format(job.getRemainingSnps()) + "<br>");

			if (job.getRemovedChunksSnps() > 0) {

				text.append("<br><b>Warning:</b> "
						+ formatter.format(job.getRemovedChunksSnps())

						+ " Chunks excluded: < 3 SNPs (see "
						+ context.createLinkToFile("statistics")
						+ "  for details).");
			}

			if (job.getRemovedChunksCallRate() > 0) {

				text.append("<br><b>Warning:</b> "
						+ formatter.format(job.getRemovedChunksCallRate())

						+ " Chunks excluded: at least one sample has a call rate < 50% (see "
						+ context.createLinkToFile("statistics")
						+ " for details).");
			}

			if (job.getRemovedChunksOverlap() > 0) {

				text.append("<br><b>Warning:</b> "
						+ formatter.format(job.getRemovedChunksOverlap())

						+ " Chunks excluded: reference overlap < 50% (see "
						+ context.createLinkToFile("statistics")
						+ " for details).");
			}

			long excludedChunks = job.getRemovedChunksSnps()
					+ job.getRemovedChunksCallRate()
					+ job.getRemovedChunksOverlap();

			if (excludedChunks > 0) {
				text.append("<br>Remaining chunk(s): "
						+ formatter.format(chunks - excludedChunks));

			}

			if (excludedChunks == chunks) {

				text.append("<br><b>Error:</b> No chunks passed the QC step. Imputation cannot be started!");
				context.error(text.toString());

				return false;

			}
			// strand flips (normal flip + allele switch AND strand flip)
			else if (job.getStrandSwitch1() + job.getStrandSwitch3() > 100) {
				text.append("<br><b>Error:</b> More than 100 obvious strand flips have been detected. Please check strand. Imputation cannot be started!");
				context.error(text.toString());

				return false;
			}

			else {
				context.warning(text.toString());
				return true;

			}

		} else {

			context.error("QC Quality Control failed!");
			return false;

		}
	}

	private int createChunkFile(WorkflowContext context, String inputFiles,
			String chunkfile, int chunkSize) {

		String folder = getFolder(QualityControl.class);
		VcfFileUtil.setBinaries(FileUtil.path(folder, "bin"));

		String files = FileUtil.path(context.getLocalTemp(), "input");

		int chunks = 0;

		int pairId = 0;

		String[] vcfFilenames = FileUtil.getFiles(files, "*.vcf.gz$|*.vcf$");

		for (String vcfFilename : vcfFilenames) {

			try {

				List<VcfFile> vcfFiles = new Vector<VcfFile>();

				VcfFile myvcfFile = VcfFileUtil.load(vcfFilename, chunkSize,
						false);
				
				if (VcfFileUtil.isValidChromosome(myvcfFile.getChromosome())) {
					// chr 1 - 22 and Chr X
					vcfFiles.add(myvcfFile);
				} 
				
				if(myvcfFile.getChromosome().equals("X")) {
					// chr X
					context.beginTask("Check chromosome X...");
					try {
						List<VcfFile> newFiles = VcfFileUtil
								.prepareChrX(myvcfFile);

						context.endTask("<b>Sex-Check:</b>"+"\n"+"Males: "
								+ newFiles.get(0).getNoSamples() + "\n"
								+ "Females: " + newFiles.get(1).getNoSamples()+"\n"+"No Sex dedected and therefore filtered: "+(myvcfFile.getNoSamples()-newFiles.get(0).getNoSamples()-newFiles.get(1).getNoSamples()),
								Message.OK);

						vcfFiles.addAll(newFiles);
					} catch (IOException e) {
						context.endTask("Chromosome X check failed.",
								Message.ERROR);
						throw e;
					}
				}
			
				for (VcfFile vcfFile : vcfFiles) {

					// writes chunk-file

					String chromosome = vcfFile.getChromosome();
					String type = vcfFile.getType();

					String chunkfileChr = HdfsUtil.path(chunkfile, chromosome);
					HdfsLineWriter writer = new HdfsLineWriter(chunkfileChr);

					// puts converted files into hdfs
					int i = 0;
					String[] hdfsFiles = new String[vcfFile.getFilenames().length];
					for (String filename : vcfFile.getFilenames()) {
						String hdfsFile = HdfsUtil.path(context.getHdfsTemp(),
								type + "_chr" + chromosome + "_" + pairId + "_"
										+ i + "");
						HdfsUtil.put(filename, hdfsFile);
						hdfsFiles[i] = hdfsFile;
						i++;
					}

					for (int chunk : vcfFile.getChunks()) {

						int start = chunk * chunkSize + 1;
						int end = start + chunkSize - 1;

						String value = chromosome + "\t" + start + "\t" + end
								+ "\t" + type;
						for (String hdfsFile : hdfsFiles) {
							value = value + "\t" + hdfsFile;
						}

						writer.write(value);
						chunks++;
					}
					pairId++;

					writer.close();

				}

			} catch (Exception e) {
				e.printStackTrace();
				return -1;
			}

		}

		return chunks;
	}

}
