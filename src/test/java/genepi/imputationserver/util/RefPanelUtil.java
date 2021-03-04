package genepi.imputationserver.util;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.List;
import java.util.Map;

import com.esotericsoftware.yamlbeans.YamlException;
import com.esotericsoftware.yamlbeans.YamlReader;

public class RefPanelUtil {

	public static Map<String, Object> loadFromFile(String filename, String id)
			throws YamlException, FileNotFoundException {

		YamlReader reader = new YamlReader(new FileReader(filename));
		List<Map<String, Object>> panels = reader.read(List.class);
		for (Map<String, Object> panel : panels) {
			if (panel.get("id").equals(id)) {
				return panel;
			}
		}

		return null;

	}

}
