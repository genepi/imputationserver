package genepi.minicloudmac.hadoop.imputation;

import genepi.base.Tool;
import genepi.minicloudmac.hadoop.util.RefPanel;
import genepi.minicloudmac.hadoop.util.RefPanelList;

public class ImputationVcf extends Tool {

	public ImputationVcf(String[] args) {
		super(args);
	}

	@Override
	public void createParameters() {
		addParameter("input", "Hdfs input path");
		addParameter("output", "Hdfs output path");
		addParameter("reference", "reference id");
		addParameter("local", "local output");
		addParameter("chunk", "chunk-size", Tool.INTEGER);
		addParameter("window", "windo-wsize", Tool.INTEGER);
		addParameter("log", "log filename");
		addParameter("phasing","phasing algorithm");

	}

	@Override
	public void init() {
		// TODO Auto-generated method stub

	}

	@Override
	public int run() {

		String input = (String) getValue("input");
		String output = (String) getValue("output");
		String reference = (String) getValue("reference");
		String local = (String) getValue("local");
		int chunk = (Integer) getValue("chunk");
		int window = (Integer) getValue("window");
		String log = (String) getValue("log");
		String phasing = (String) getValue("phasing");

		MiniMacVcfJob job = new MiniMacVcfJob("");

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

		job.setRefPanelHdfs(panel.getHdfs());
		job.setRefPanelPattern(panel.getPattern());
		job.setInput(input);
		job.setOutput(output);
		job.setRefPanel(reference);
		job.setLocalOutput(local);
		job.setChunkSize(chunk);
		job.setWindowSize(window);
		job.setLogFilename(log);
		job.setPhasing(phasing);

		if (job.execute()) {
			return 0;
		} else {
			return 1;
		}
	}

}
