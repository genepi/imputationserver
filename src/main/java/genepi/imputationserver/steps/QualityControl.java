package genepi.imputationserver.steps;

import genepi.base.Tool;
import genepi.imputationserver.steps.qc.QualityControlJob;
import genepi.imputationserver.util.RefPanel;
import genepi.imputationserver.util.RefPanelList;

public class QualityControl extends Tool {

	public QualityControl(String[] args) {
		super(args);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void createParameters() {
		addParameter("input", "Hdfs input path");
		addParameter("reference", "reference id");
		addParameter("output", "Hdfs output path");
		addParameter("output-manifest", "output-manifest folder");
		addParameter("population", "reference population");
		addParameter("qcstat", "qc stat output");
	}

	@Override
	public void init() {

	}

	@Override
	public int run() {

		String input = (String) getValue("input");
		String output = (String) getValue("output");

		String outputManifest = (String) getValue("output-manifest");

		String reference = (String) getValue("reference");
		String population = (String) getValue("population");
		String qcstat = (String) getValue("qcstat");
		
		RefPanelList panels = null;
		try {
			panels = RefPanelList.loadFromFile("panels.txt");

		} catch (Exception e) {

			System.out.println("panels.txt not found.");
			return 1;
		}
		RefPanel panel = panels.getById(reference);
		if (panel == null) {
			System.out.println("Reference '" + reference + "' not found.");
			System.out.println("Available references:");
			for (RefPanel p : panels.getPanels()) {
				System.out.println(p.getId());
			}

			return 1;
		}

		QualityControlJob job = new QualityControlJob("maf");
		job.setLegendHdfs(panel.getLegend());
		job.setLegendPattern(panel.getLegendPattern());
		job.setPopulation(population);
		job.setInput(input);
		job.setOutput(output + "_temp");
		job.setOutputMaf(output);
		job.setOutputManifest(outputManifest);
		job.setQcStat(qcstat);

		if (job.execute()) {
			return 0;
		} else {
			return 1;
		}

	}

}
