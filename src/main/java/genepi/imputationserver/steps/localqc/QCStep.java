package genepi.imputationserver.steps.localQC;

import genepi.hadoop.PreferenceStore;
import genepi.hadoop.common.WorkflowContext;
import genepi.hadoop.common.WorkflowStep;
import genepi.imputationserver.util.RefPanel;
import genepi.imputationserver.util.RefPanelList;
import genepi.io.FileUtil;
import java.io.File;
import java.io.IOException;

public class QCStep extends WorkflowStep {

	@Override
	public boolean run(WorkflowContext context) {

		String folder = getFolder(QCStep.class);

		String inputFiles = context.get("files");
		String reference = context.get("refpanel");
		String population = context.get("population");
		String output = context.get("qcOutput");
		int chunkSize = Integer.parseInt(context.get("chunksize"));

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

		QCChecker qcChecker = new QCChecker();
		qcChecker.setPrefix(output);
		qcChecker.setInput(inputFiles);
		qcChecker.setChunkSize(chunkSize);
		qcChecker.setPhasingWindow(phasingWindow);
		qcChecker.setPopulation(population);
		qcChecker.setLegendFile(panel.getLegend());

		context.beginTask("QC...");
		
		try {

			qcChecker.start();
			context.endTask("QC done", WorkflowContext.OK);
			return true;

		} catch (Exception e) {
			context.error(e.getMessage());
			context.endTask("QC Error", WorkflowContext.ERROR);
			return false;
		}

	}

}
