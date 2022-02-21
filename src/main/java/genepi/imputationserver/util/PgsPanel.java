package genepi.imputationserver.util;

import java.util.List;
import java.util.Map;
import java.util.Vector;

import genepi.hadoop.HdfsUtil;

public class PgsPanel {

	private String location = "";

	private String build = "";

	private List<String> scores = new Vector<>();

	private PgsPanel() {

	}

	public static PgsPanel loadFromProperties(Object properties) {
		if (properties != null) {
			Map<String, Object> map = (Map<String, Object>) properties;

			PgsPanel panel = new PgsPanel();
			if (map.containsKey("location")) {
				panel.location = map.get("location").toString();
			}
			if (map.containsKey("build")) {
				panel.build = map.get("build").toString();
			}
			if (map.containsKey("scores")) {
				List<String> list = (List<String>) map.get("scores");
				panel.scores = list;
				return panel;
			} else {
				return null;
			}
		} else {
			return null;
		}

	}

	public List<String> getScores() {
		List<String> scoresPath = new Vector<String>();
		for (String score : scores) {
			scoresPath.add(HdfsUtil.path(location, score));
		}
		return scoresPath;
	}

	public String getLocation() {
		return location;
	}

	public String getBuild() {
		return build;
	}

}
