package genepi.imputationserver.steps;

import genepi.hadoop.HdfsUtil;
import genepi.hadoop.PreferenceStore;
import genepi.hadoop.io.HdfsLineWriter;
import genepi.imputationserver.steps.qc.QualityControlJob;
import genepi.imputationserver.steps.vcf.VcfFile;
import genepi.imputationserver.steps.vcf.VcfFileUtil;
import genepi.imputationserver.util.HadoopJobStep;
import genepi.imputationserver.util.RefPanel;
import genepi.imputationserver.util.RefPanelList;
import genepi.io.FileUtil;

import java.io.File;
import java.text.DecimalFormat;

import cloudgene.mapred.jobs.CloudgeneContext;
import cloudgene.mapred.wdl.WdlStep;

public class QualityControl extends HadoopJobStep {

	@Override
	public boolean run(WdlStep step, CloudgeneContext context) {

		String folder = getFolder(HadoopJobStep.class);

		// inputs
		String reference = context.get("refpanel");
		String population = context.get("population");
		String files = context.get("files");
		String chunkfile = context.get("chunkfile");

		// outputs
		String output = context.get("outputmaf");
		String outputManifest = context.get("mafchunkfile");
		String removedSnps = context.get("filtered");

		// read config
		PreferenceStore store = new PreferenceStore(new File(FileUtil.path(
				folder, "job.config")));
		int chunkSize = Integer.parseInt(store.getString("pipeline.chunksize"));

		// create manifest file
		int chunks = createChunkFile(context, files, chunkfile, chunkSize);

		if (chunks == -1) {
			error("Error during manifest file creation.");
			return false;
		}

		// load reference panels
		RefPanelList panels = null;
		try {
			panels = RefPanelList.loadFromFile(FileUtil.path(folder,
					"panels.txt"));

		} catch (Exception e) {

			error("panels.txt not found.");
			return false;
		}

		// check reference panel
		RefPanel panel = panels.getById(reference);
		if (panel == null) {
			error("Reference '" + reference + "' not found.");
			error("Available references:");
			for (RefPanel p : panels.getPanels()) {
				error(p.getId());
			}

			return false;
		}

		// submit qc hadoop job
		QualityControlJob job = new QualityControlJob("maf");
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

		if (successful) {

			// print qc statistics
			DecimalFormat df = new DecimalFormat("#.00");

			StringBuffer text = new StringBuffer();

			text.append("<b>Statistics:</b> <br>");
			text.append("Alternative allele frequency > 0.5 sites: "
					+ job.getAlternativeAlleles() + "<br>");
			text.append("Reference Overlap: "
					+ df.format(job.getFoundInLegend()
							/ (double) (job.getFoundInLegend() + job
									.getNotFoundInLegend()) * 100) + "% "
					+ "<br>");

			text.append("<b>Filter details:</b> <br>");
			text.append("Filter flag set: " + job.getFilterFlag() + "<br>");
			text.append("Invalid alleles: " + job.getInvalidAlleles() + "<br>");
			text.append("Duplicated sites: " + job.getDuplicates() + "<br>");
			text.append("NonSNP sites: " + job.getNoSnps() + "<br>");
			text.append("Monomorphic sites: " + job.getMonomorphic() + "<br>");
			text.append("Allele mismatch: " + job.getAlleleMismatch() + "<br>");
			text.append("SNPs call rate < 90%: " + job.getToLessSamples());

			ok(text.toString());

			text = new StringBuffer();

			text.append("Excluded sites in total: " + job.getFiltered()
					+ "<br>");
			text.append("Remaining sites in total: " + job.getRemainingSnps()
					+ "<br>");

			if (job.getRemovedChunksSnps() > 0) {

				text.append("<b>Warning:</b> " + job.getRemovedChunksSnps()

				+ " Chunks excluded: < 3 SNPs (see " + context.createLinkToFile("filtered") + "  for details).");
			}

			if (job.getRemovedChunksCallRate() > 0) {

				text.append("<b>Warning:</b> "
						+ job.getRemovedChunksCallRate()

						+ " Chunks excluded: at least one sample has a call rate < 50% (see " + context.createLinkToFile("filtered") + " for details).");
			}

			if (job.getRemovedChunksOverlap() > 0) {

				text.append("<b>Warning:</b> "
						+ job.getRemovedChunksOverlap()

						+ " Chunks excluded: reference overlap < 50% (see " + context.createLinkToFile("filtered") + " for details).");
			}

			long excludedChunks = job.getRemovedChunksSnps()
					+ job.getRemovedChunksCallRate()
					+ job.getRemovedChunksOverlap();

			if (excludedChunks > 0) {
				text.append("<br>Remaining chunk(s): "
						+ (chunks - excludedChunks));

			}

			if (excludedChunks == chunks) {

				text.append("<br><b>Error:</b> No chunks passed the QC step. Imputation cannot be started!");
				error(text.toString());

				return false;

			} else {
				warning(text.toString());
				return true;

			}

		} else {

			error("QC Quality Control failed!");
			return false;

		}
	}

	private int createChunkFile(CloudgeneContext context, String inputFiles,
			String chunkfile, int chunkSize) {
		String files = FileUtil.path(context.getLocalTemp(), "input");

		// exports files from hdfs
		try {

			HdfsUtil.getFolder(inputFiles, files);

		} catch (Exception e) {
			error("Downloading files: " + e.getMessage());
			return -1;

		}

		int chunks = 0;

		int pairId = 0;

		String[] vcfFiles = FileUtil.getFiles(files, "*.vcf.gz$|*.vcf$");

		for (String filename : vcfFiles) {

			try {

				VcfFile vcfFile = VcfFileUtil.load(filename, chunkSize, null);

				if (VcfFileUtil.isAutosomal(vcfFile.getChromosome())) {

					// writes chunk-file

					String chromosome = vcfFile.getChromosome();
					String type = vcfFile.getType();

					String chunkfileChr = HdfsUtil.path(chunkfile, chromosome);
					HdfsLineWriter writer = new HdfsLineWriter(chunkfileChr);

					// puts converted files into hdfs
					int i = 0;
					String[] hdfsFiles = new String[vcfFile.getFilenames().length];
					for (String filename2 : vcfFile.getFilenames()) {
						String hdfsFile = HdfsUtil.path(context.getHdfsTemp(),
								type + "_chr" + chromosome + "_" + pairId + "_"
										+ i + "");
						HdfsUtil.put(filename2, hdfsFile);
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
