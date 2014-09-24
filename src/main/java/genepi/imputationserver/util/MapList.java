package genepi.imputationserver.util;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.List;
import java.util.Vector;

import com.esotericsoftware.yamlbeans.YamlException;
import com.esotericsoftware.yamlbeans.YamlReader;

public class MapList {

	private List<GeneticMap> maps;

	public MapList() {
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

	public static MapList loadFromFile(String filename)
			throws YamlException, FileNotFoundException {

		if (new File(filename).exists()) {

			YamlReader reader = new YamlReader(new FileReader(filename));
			reader.getConfig().setPropertyElementType(MapList.class,
					"maps", GeneticMap.class);
			reader.getConfig().setClassTag(
					"genepi.minicloudmac.hadoop.util.MapList",
					MapList.class);
			MapList result = reader.read(MapList.class);
			return result;
		} else {
			return new MapList();
		}

	}

}
