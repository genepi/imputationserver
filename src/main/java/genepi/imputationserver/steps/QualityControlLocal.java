package genepi.imputationserver.steps;

import genepi.hadoop.PreferenceStore;
import genepi.hadoop.common.WorkflowContext;
import genepi.hadoop.common.WorkflowStep;
import genepi.imputationserver.steps.qc.QCStatistics;
import genepi.imputationserver.util.RefPanel;
import genepi.imputationserver.util.RefPanelList;
import genepi.io.FileUtil;
import java.io.File;
import java.text.DecimalFormat;

public class QualityControlLocal extends WorkflowStep {

	@Override
	public boolean run(WorkflowContext context) {

		String folder = getFolder(QualityControlLocal.class);

		String inputFiles = context.get("files");
		String reference = context.get("refpanel");
		String population = context.get("population");
		int chunkSize = Integer.parseInt(context.get("chunksize"));

		String outputMaf = context.get("outputmaf");
		String outputChunkfile = context.get("mafchunkfile");
		String excluded = context.get("statistics");
		String chunks = context.get("chunks");

		PreferenceStore store = new PreferenceStore(new File(FileUtil.path(folder, "job.config")));
		int phasingWindow = Integer.parseInt(store.getString("phasing.window"));

		// load reference panels
		RefPanelList panels = null;
		try {
			panels = RefPanelList.loadFromFile(FileUtil.path(folder, "panels.txt"));

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

		QCStatistics qcStats = new QCStatistics();

		qcStats.setOutputMaf(outputMaf);
		qcStats.setChunkfile(outputChunkfile);
		qcStats.setChunks(chunks);
		qcStats.setExcludeLog(excluded);
		qcStats.setInput(inputFiles);
		qcStats.setChunkSize(chunkSize);
		qcStats.setPhasingWindow(phasingWindow);
		qcStats.setPopulation(population);
		qcStats.setLegendFile(panel.getLegend());

		context.beginTask("Calculating QC Statistics...");

		boolean successful;
		try {

			successful = qcStats.start();

		} catch (Exception e) {
			context.endTask(e.getMessage(), WorkflowContext.ERROR);
			return false;
		}

		if (successful) {

			context.endTask("QC finished successfully", WorkflowContext.OK);

			DecimalFormat df = new DecimalFormat("#.00");
			DecimalFormat formatter = new DecimalFormat("###,###.###");

			StringBuffer text = new StringBuffer();

			text.append("<b>Statistics:</b> <br>");
			text.append("Alternative allele frequency > 0.5 sites: " + formatter.format(qcStats.getAlternativeAlleles())
					+ "<br>");
			text.append("Reference Overlap: " + qcStats.getFoundInLegend() + "/"
					+ ((qcStats.getFoundInLegend() + qcStats.getNotFoundInLegend())) + " ("
					+ df.format(qcStats.getFoundInLegend()
							/ (double) (qcStats.getFoundInLegend() + qcStats.getNotFoundInLegend()) * 100)
					+ "%" + ")<br>");

			text.append("Match: " + formatter.format(qcStats.getMatch()) + "<br>");
			text.append("Allele switch: " + formatter.format(qcStats.getAlleleSwitch()) + "<br>");
			text.append("Strand flip: " + formatter.format(qcStats.getStrandSwitch1()) + "<br>");
			text.append("Strand flip and allele switch: " + formatter.format(qcStats.getStrandSwitch3()) + "<br>");
			text.append("A/T, C/G genotypes: " + formatter.format(qcStats.getStrandSwitch2()) + "<br>");

			text.append("<b>Filtered sites:</b> <br>");
			text.append("Filter flag set: " + formatter.format(qcStats.getFilterFlag()) + "<br>");
			text.append("Invalid alleles: " + formatter.format(qcStats.getInvalidAlleles()) + "<br>");
			text.append("Multiallelic sites: " + formatter.format(qcStats.getMultiallelicSites()) + "<br>");
			text.append("Duplicated sites: " + formatter.format(qcStats.getDuplicates()) + "<br>");
			text.append("NonSNP sites: " + formatter.format(qcStats.getNoSnps()) + "<br>");
			text.append("Monomorphic sites: " + formatter.format(qcStats.getMonomorphic()) + "<br>");
			text.append("Allele mismatch: " + formatter.format(qcStats.getAlleleMismatch()) + "<br>");
			text.append("SNPs call rate < 90%: " + formatter.format(qcStats.getLowCallRate()));

			context.ok(text.toString());

			text = new StringBuffer();

			text.append("Excluded sites in total: " + formatter.format(qcStats.getFiltered()) + "<br>");
			text.append("Remaining sites in total: " + formatter.format(qcStats.getOverallSnps()) + "<br>");

			if (qcStats.getRemovedChunksSnps() > 0) {

				text.append("<br><b>Warning:</b> " + formatter.format(qcStats.getRemovedChunksSnps())

						+ " Chunk(s) excluded: < 3 SNPs (see " + context.createLinkToFile("statistics")
						+ "  for details).");
			}

			if (qcStats.getRemovedChunksCallRate() > 0) {

				text.append("<br><b>Warning:</b> " + formatter.format(qcStats.getRemovedChunksCallRate())

						+ " Chunk(s) excluded: at least one sample has a call rate < 50% (see "
						+ context.createLinkToFile("statistics") + " for details).");
			}

			if (qcStats.getRemovedChunksOverlap() > 0) {

				text.append("<br><b>Warning:</b> " + formatter.format(qcStats.getRemovedChunksOverlap())

						+ " Chunk(s) excluded: reference overlap < 50% (see " + context.createLinkToFile("statistics")
						+ " for details).");
			}

			long excludedChunks = qcStats.getRemovedChunksSnps() + qcStats.getRemovedChunksCallRate()
					+ qcStats.getRemovedChunksOverlap();

			long amountChunks = qcStats.getAmountChunks();

			if (excludedChunks > 0) {
				text.append("<br>Remaining chunk(s): " + formatter.format(amountChunks - excludedChunks));

			}

			if (excludedChunks == amountChunks) {

				text.append("<br><b>Error:</b> No chunks passed the QC step. Imputation cannot be started!");
				context.error(text.toString());

				return false;

			}
			// strand flips (normal flip + allele switch AND strand flip)
			else if (qcStats.getStrandSwitch1() + qcStats.getStrandSwitch3() > 100) {
				text.append(
						"<br><b>Error:</b> More than 100 obvious strand flips have been detected. Please check strand. Imputation cannot be started!");
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

}
