package genepi.minicloudmac.hadoop.install;

import genepi.base.Tool;
import genepi.hadoop.HdfsUtil;
import genepi.io.FileUtil;
import genepi.minicloudmac.hadoop.util.RefPanel;
import genepi.minicloudmac.hadoop.util.RefPanelList;

import java.io.File;
import java.io.IOException;

public class Install extends Tool {

	public Install(String[] args) {
		super(args);
	}

	@Override
	public void createParameters() {

		addParameter("id", "identifier");
		addParameter("tgz", "reference panel (tgz).");
		addParameter("pattern", "pattern");

	}

	@Override
	public void init() {
		// TODO Auto-generated method stub

	}

	@Override
	public int run() {

		String id = (String) getValue("id");
		String tgz = (String) getValue("tgz");
		String pattern = (String) getValue("pattern");

		String hdfs = HdfsUtil.path("minimac-data", id + ".tgz");
		String panelFile = FileUtil.path("panels.txt");

		System.out.println("Import panel...");
		HdfsUtil.put(tgz, hdfs);

		System.out.println("Update config file " + panelFile + "...");

		RefPanelList panels = null;
		try {
			panels = RefPanelList.loadFromFile(panelFile);
		} catch (IOException e) {
			System.out.println("Error reading file: " + panelFile);
			e.printStackTrace();
			return 1;
		}

		RefPanel panel = new RefPanel();
		panel.setId(id);
		panel.setHdfs(hdfs);
		panel.setPattern(pattern);

		panels.add(panel);
		try {
			RefPanelList.saveToFile(panelFile, panels);
		} catch (IOException e) {
			System.out.println("Error writing file: " + panelFile);
			e.printStackTrace();
		}

		return 0;

	}

	public static String getFolder(Class clazz) {
		return new File(clazz.getProtectionDomain().getCodeSource()
				.getLocation().getPath()).getParent();
	}

}
