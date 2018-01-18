package genepi.imputationserver.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import com.esotericsoftware.yamlbeans.YamlException;
import com.esotericsoftware.yamlbeans.YamlReader;
import com.esotericsoftware.yamlbeans.YamlWriter;

public class RefPanelList {

	public static String FILENAME = "panels.txt";

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

	public RefPanel getById(String id, Object properties) {

		if (properties != null) {
			RefPanel panel = new RefPanel();
			Map<String, String> map = (Map<String, String>) properties;
			panel.setBuild(map.get("build"));
			panel.setHdfs(map.get("hdfs"));
			panel.setId(map.get("id"));
			panel.setLegend(map.get("legend"));
			panel.setMapEagle(map.get("mapEagle"));
			panel.setMapHapiUR(map.get("mapHapiUR"));
			panel.setMapMinimac(map.get("mapMinimac"));
			panel.setMapPatternHapiUR(map.get("mapPatternHapiUR"));
			panel.setMapPatternShapeIT(map.get("mapPatternShapeIT"));
			panel.setMapShapeIT(map.get("mapShapeIT"));
			panel.setRefEagle(map.get("refEagle"));
			return panel;
		}
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

	public static RefPanelList loadFromFile(String filename) {

		if (new File(filename).exists()) {
			try {
				YamlReader reader = new YamlReader(new FileReader(filename));
				reader.getConfig().setPropertyElementType(RefPanelList.class, "panels", RefPanel.class);
				reader.getConfig().setClassTag("genepi.minicloudmac.hadoop.util.RefPanelList", RefPanelList.class);
				RefPanelList result = reader.read(RefPanelList.class);
				return result;
			} catch (Exception e) {
				return new RefPanelList();
			}
		} else {
			return new RefPanelList();
		}

	}

	public static void saveToFile(String filename, RefPanelList panels) throws IOException {

		YamlWriter writer = new YamlWriter(new FileWriter(filename));
		writer.getConfig().setClassTag("genepi.minicloudmac.hadoop.util.RefPanelList", RefPanelList.class);
		writer.getConfig().setPropertyElementType(RefPanelList.class, "panels", RefPanel.class);
		writer.write(panels);
		writer.close();

	}

}
