package genepi.imputationserver.util;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.List;
import java.util.Vector;

import com.esotericsoftware.yamlbeans.YamlException;
import com.esotericsoftware.yamlbeans.YamlReader;

public class GeneticMapList {

	public static String FILENAME = "genetic-maps-m4.txt";
	
	private List<GeneticMap> maps;

	public GeneticMapList() {
		maps = new Vector<GeneticMap>();
	}

	public List<GeneticMap> getMaps() {
		return maps;
	}

	public void setMaps(List<GeneticMap> maps) {
		this.maps = maps;
	}

	public GeneticMap getById(String id) {
		for (GeneticMap map : maps) {
			if (map.getId().equals(id)) {
				return map;
			}
		}
		return null;
	}

	public void add(GeneticMap map) {
		maps.add(map);
	}

	public static GeneticMapList loadFromFile(String filename)
			throws YamlException, FileNotFoundException {

		if (new File(filename).exists()) {

			YamlReader reader = new YamlReader(new FileReader(filename));
			reader.getConfig().setPropertyElementType(GeneticMapList.class,
					"maps", GeneticMap.class);
			reader.getConfig().setClassTag(
					"genepi.minicloudmac.hadoop.util.MapList",
					GeneticMapList.class);
			GeneticMapList result = reader.read(GeneticMapList.class);
			return result;
		} else {
			return new GeneticMapList();
		}

	}

}
