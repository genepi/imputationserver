package genepi.imputationserver.util;

import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.stream.Collectors;

public class PgsPanel {

	private List<String> scores = new Vector<>();

	private PgsPanel() {

	}

	public static PgsPanel loadFromProperties(Object properties) {
		if (properties != null) {
			Map<String, Object> map = (Map<String, Object>) properties;
			if (map.get("scores") != null) {
				List<String> list = (List<String>) map.get("scores");
				PgsPanel panel = new PgsPanel();
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
		return scores;
	}

	public String getScoresAsString() {
		if (scores != null) {
			return scores.stream().collect(Collectors.joining(","));
		} else {
			return "";
		}
	}

}
