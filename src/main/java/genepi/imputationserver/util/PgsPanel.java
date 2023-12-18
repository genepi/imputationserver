package genepi.imputationserver.util;

import java.util.Map;

import genepi.hadoop.HdfsUtil;

public class PgsPanel {

	private String location = "";

	private String build = "";

	private String meta = null;

	private String scores = null;

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
			if (map.containsKey("meta")) {
				panel.meta = map.get("meta").toString();
			}
			if (map.containsKey("scores")) {
				panel.scores = map.get("scores").toString();
				return panel;
			} else {
				return null;
			}
		} else {
			return null;
		}

	}

	public String getScores() {
		String scoresPath = HdfsUtil.path(scores);
		return scoresPath;
	}

	public String getLocation() {
		return location;
	}

	public String getBuild() {
		return build;
	}

	public String getMeta() {
		return meta;
	}

	public void setMeta(String meta) {
		this.meta = meta;
	}

}
