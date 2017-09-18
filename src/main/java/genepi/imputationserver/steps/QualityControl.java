package genepi.imputationserver.steps;

import genepi.hadoop.PreferenceStore;
import genepi.hadoop.common.WorkflowContext;
import genepi.hadoop.common.WorkflowStep;
import genepi.imputationserver.steps.qc.QCStatistics;
import genepi.imputationserver.steps.vcf.VcfFileUtil;
import genepi.imputationserver.util.GenomicTools;
import genepi.imputationserver.util.QualityControlObject;
import genepi.imputationserver.util.RefPanel;
import genepi.imputationserver.util.RefPanelList;
import genepi.io.FileUtil;
import java.io.File;
import java.text.DecimalFormat;

public class QualityControl extends WorkflowStep {

	
	protected void setupTabix(String folder){
		VcfFileUtil.setBinary(FileUtil.path(folder, "bin", "tabix"));
	}
	
	@Override
	public boolean run(WorkflowContext context) {

		String folder = getFolder(QualityControl.class);
		setupTabix(folder);
		String inputFiles = context.get("files");
		String reference = context.get("refpanel");
		String population = context.get("population");
		int chunkSize = Integer.parseInt(context.get("chunksize"));

		String mafFile = context.get("mafFile");
		String chunkFileDir = context.get("chunkFileDir");
		String statDir = context.get("statisticDir");
		String chunksDir = context.get("chunksDir");

		PreferenceStore store = new PreferenceStore(new File(FileUtil.path(folder, "job.config")));
		int phasingWindow = Integer.parseInt(store.getString("phasing.window"));

		// load reference panels
		RefPanelList panels = null;
		try {
			panels = RefPanelList.loadFromFile(FileUtil.path(folder, RefPanelList.FILENAME));

		} catch (Exception e) {

			context.error("File " + RefPanelList.FILENAME + " not found.");
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

		int referenceSamples = GenomicTools.getPanelSize(reference);
		QCStatistics qcStatistics = new QCStatistics();

		qcStatistics.setInput(inputFiles);

		qcStatistics.setChunkSize(chunkSize);
		qcStatistics.setPhasingWindow(phasingWindow);
		qcStatistics.setPopulation(population);
		qcStatistics.setLegendFile(panel.getLegend());
		qcStatistics.setRefSamples(referenceSamples);

		qcStatistics.setMafFile(mafFile);
		qcStatistics.setChunkFileDir(chunkFileDir);
		qcStatistics.setChunksDir(chunksDir);
		qcStatistics.setStatDir(statDir);

		context.beginTask("Calculating QC Statistics...");

		QualityControlObject answer;

		try {

			answer = qcStatistics.run();

		} catch (Exception e) {
			context.endTask(e.getMessage(), WorkflowContext.ERROR);
			return false;
		}

		if (answer.isSuccess()) {

			context.endTask("QC executed successfully", WorkflowContext.OK);

			DecimalFormat df = new DecimalFormat("#.00");
			DecimalFormat formatter = new DecimalFormat("###,###.###");

			StringBuffer text = new StringBuffer();

			text.append("<b>Statistics:</b> <br>");
			text.append("Alternative allele frequency > 0.5 sites: "
					+ formatter.format(qcStatistics.getAlternativeAlleles()) + "<br>");
			text.append("Reference Overlap: "
					+ df.format(qcStatistics.getFoundInLegend()
							/ (double) (qcStatistics.getFoundInLegend() + qcStatistics.getNotFoundInLegend()) * 100)
					+ " %" + "<br>");

			text.append("Match: " + formatter.format(qcStatistics.getMatch()) + "<br>");
			text.append("Allele switch: " + formatter.format(qcStatistics.getAlleleSwitch()) + "<br>");
			text.append("Strand flip: " + formatter.format(qcStatistics.getStrandFlipSimple()) + "<br>");
			text.append("Strand flip and allele switch: "
					+ formatter.format(qcStatistics.getStrandFlipAndAlleleSwitch()) + "<br>");
			text.append("A/T, C/G genotypes: " + formatter.format(qcStatistics.getComplicatedGenotypes()) + "<br>");

			text.append("<b>Filtered sites:</b> <br>");
			text.append("Filter flag set: " + formatter.format(qcStatistics.getFilterFlag()) + "<br>");
			text.append("Invalid alleles: " + formatter.format(qcStatistics.getInvalidAlleles()) + "<br>");
			text.append("Multiallelic sites: " + formatter.format(qcStatistics.getMultiallelicSites()) + "<br>");
			text.append("Duplicated sites: " + formatter.format(qcStatistics.getDuplicates()) + "<br>");
			text.append("NonSNP sites: " + formatter.format(qcStatistics.getNoSnps()) + "<br>");
			text.append("Monomorphic sites: " + formatter.format(qcStatistics.getMonomorphic()) + "<br>");
			text.append("Allele mismatch: " + formatter.format(qcStatistics.getAlleleMismatch()) + "<br>");
			text.append("SNPs call rate < 90%: " + formatter.format(qcStatistics.getLowCallRate()));

			context.ok(text.toString());

			text = new StringBuffer();

			text.append("Excluded sites in total: " + formatter.format(qcStatistics.getFiltered()) + "<br>");
			text.append("Remaining sites in total: " + formatter.format(qcStatistics.getOverallSnps()) + "<br>");
			text.append("See " + context.createLinkToFile("statisticDir","snps-excluded.txt")
			+ " for details"+ "<br>");

			if (qcStatistics.getRemovedChunksSnps() > 0) {

				text.append("<br><b>Warning:</b> " + formatter.format(qcStatistics.getRemovedChunksSnps())

						+ " Chunk(s) excluded: < 3 SNPs (see " + context.createLinkToFile("statisticDir", "chunks-excluded.txt")
						+ "  for details).");
			}

			if (qcStatistics.getRemovedChunksCallRate() > 0) {

				text.append("<br><b>Warning:</b> " + formatter.format(qcStatistics.getRemovedChunksCallRate())

						+ " Chunk(s) excluded: at least one sample has a call rate < 50% (see "
						+ context.createLinkToFile("statisticDir", "chunks-excluded.txt") + " for details).");
			}

			if (qcStatistics.getRemovedChunksOverlap() > 0) {

				text.append("<br><b>Warning:</b> " + formatter.format(qcStatistics.getRemovedChunksOverlap())

						+ " Chunk(s) excluded: reference overlap < 50% (see " + context.createLinkToFile("statisticDir","chunks-excluded.txt")
						+ " for details).");
			}

			long excludedChunks = qcStatistics.getRemovedChunksSnps() + qcStatistics.getRemovedChunksCallRate()
					+ qcStatistics.getRemovedChunksOverlap();

			long overallChunks = qcStatistics.getOverallChunks();

			if (excludedChunks > 0) {
				text.append("<br>Remaining chunk(s): " + formatter.format(overallChunks - excludedChunks));

			}

			if (excludedChunks == overallChunks) {

				text.append("<br><b>Error:</b> No chunks passed the QC step. Imputation cannot be started!");
				context.error(text.toString());

				return false;

			}
			// strand flips (normal flip & allele switch + strand flip)
			else if (qcStatistics.getStrandFlipSimple() + qcStatistics.getStrandFlipAndAlleleSwitch() > 100) {
				text.append(
						"<br><b>Error:</b> More than 100 obvious strand flips have been detected. Please check strand. Imputation cannot be started!");
				context.error(text.toString());

				return false;
			}

			else {
				
				text.append(answer.getMessage());
				context.warning(text.toString());
				return true;

			}

		} else {

			context.endTask("QC failed!", WorkflowContext.ERROR);
			context.error(answer.getMessage());
			return false;

		}

	}

}
