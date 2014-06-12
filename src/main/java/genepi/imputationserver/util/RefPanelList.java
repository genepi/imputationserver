package genepi.imputationserver.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Vector;

import com.esotericsoftware.yamlbeans.YamlException;
import com.esotericsoftware.yamlbeans.YamlReader;
import com.esotericsoftware.yamlbeans.YamlWriter;

public class RefPanelList {

	private List<RefPanel> panels;

	public RefPanelList() {
		panels = new Vector<RefPanel>();
	}

	public List<RefPanel> getPanels() {
		return panels;
	}

	public void setPanels(List<RefPanel> panels) {
		this.panels = panels;
	}

	public RefPanel getById(String id) {
		for (RefPanel panel : panels) {
			if (panel.getId().equals(id)) {
				return panel;
			}
		}
		return null;
	}

	public void add(RefPanel panel) {
		panels.add(panel);
	}

	public static RefPanelList loadFromFile(String filename)
			throws YamlException, FileNotFoundException {

		if (new File(filename).exists()) {

			YamlReader reader = new YamlReader(new FileReader(filename));
			reader.getConfig().setPropertyElementType(RefPanelList.class,
					"panels", RefPanel.class);
			reader.getConfig().setClassTag(
					"genepi.minicloudmac.hadoop.util.RefPanelList",
					RefPanelList.class);
			RefPanelList result = reader.read(RefPanelList.class);
			return result;
		} else {
			return new RefPanelList();
		}

	}

	public static void saveToFile(String filename, RefPanelList panels)
			throws IOException {

		YamlWriter writer = new YamlWriter(new FileWriter(filename));
		writer.getConfig().setClassTag(
				"genepi.minicloudmac.hadoop.util.RefPanelList",
				RefPanelList.class);
		writer.getConfig().setPropertyElementType(RefPanelList.class, "panels",
				RefPanel.class);
		writer.write(panels);
		writer.close();

	}

}
