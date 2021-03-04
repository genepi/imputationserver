package genepi.imputationserver.util;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import genepi.hadoop.HdfsUtil;

public class RefPanel {

	public static final int MAX_STRAND_FLIPS = 100;

	public static final double MIN_SAMPLE_CALL_RATE = 0.5;

	public static final int MIN_SNPS = 3;

	public static final double MIN_OVERLAP = 0.5;

	public static final double CHR_X_MIXED_GENOTYPES = 0.1;

	public static final double MIN_SAMPLES_MONOMORPHIC = 2;

	private String id;

	private String hdfs;

	private String legend;

	private String mapMinimac;

	private String build = "hg19";

	private String mapEagle;

	private String refEagle;

	private String refBeagle;

	private String mapBeagle;

	private Map<String, String> samples;

	private Map<String, String> populations;

	private Map<String, String> defaultQcFilter;

	private Map<String, String> qcFilter;

	private String range;

	/**
	 * 
	 */
	public RefPanel() {
		defaultQcFilter = new HashMap<String, String>();
		defaultQcFilter.put("overlap", MIN_OVERLAP + "");
		defaultQcFilter.put("minSnps", MIN_SNPS + "");
		defaultQcFilter.put("sampleCallrate", MIN_SAMPLE_CALL_RATE + "");
		defaultQcFilter.put("mixedGenotypeschrX", CHR_X_MIXED_GENOTYPES + "");
		defaultQcFilter.put("strandFlips", MAX_STRAND_FLIPS + "");
		defaultQcFilter.put("minSamplesMonomorphic", MIN_SAMPLES_MONOMORPHIC + "");
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getHdfs() {
		return hdfs;
	}

	public String getVersion() {

		FileStatus status;
		try {
			status = FileSystem.get(HdfsUtil.getConfiguration()).getFileStatus(new Path(hdfs));
			return new Date(status.getModificationTime()).toString() + " (" + status.getLen() + " Bytes)";
		} catch (IOException e) {
			return "??";
		}

	}

	public void setHdfs(String hdfs) {
		this.hdfs = hdfs;
	}

	public String getLegend() {
		return legend;
	}

	public void setLegend(String legend) {
		this.legend = legend;
	}

	public void setBuild(String build) {
		this.build = build;
	}

	public String getBuild() {
		return build;
	}

	public void setMapMinimac(String mapMinimac) {
		this.mapMinimac = mapMinimac;
	}

	public String getMapMinimac() {
		return mapMinimac;
	}

	public void setMapEagle(String mapEagle) {
		this.mapEagle = mapEagle;
	}

	public String getMapEagle() {
		return mapEagle;
	}

	public String getRefBeagle() {
		return refBeagle;
	}

	public void setRefBeagle(String refBeagle) {
		this.refBeagle = refBeagle;
	}

	public void setRefEagle(String refEagle) {
		this.refEagle = refEagle;
	}

	public String getRefEagle() {
		return refEagle;
	}

	public boolean checkEagleMap() {

		if (mapEagle == null) {
			return false;
		}

		return HdfsUtil.exists(mapEagle);

	}

	public boolean checkEagleBcf() {

		if (refEagle == null) {
			return false;
		}

		return HdfsUtil.exists(refEagle);

	}

	public void setSamples(Map<String, String> samples) {
		this.samples = samples;
	}

	public Map<String, String> getSamples() {
		return samples;
	}

	public int getSamplesByPopulation(String population) {
		if (samples == null) {
			return 0;
		}
		String n = samples.get(population);
		if (n != null) {
			return Integer.parseInt(n);
		} else {
			return 0;
		}
	}

	public void setPopulations(Map<String, String> populations) {
		this.populations = populations;
	}

	public Map<String, String> getPopulations() {
		return populations;
	}

	public boolean supportsPopulation(String population) {

		if (population == null || population.equals("")) {
			return true;
		}

		if (populations == null) {
			return false;
		} else {
			return populations.containsKey(population);
		}

	}

	public Map<String, String> getQcFilter() {
		return qcFilter;
	}

	public double getQcFilterByKey(String key) {
		if (qcFilter == null) {
			qcFilter = defaultQcFilter;
		}
		String n = qcFilter.get(key);
		if (n != null) {
			return Double.parseDouble(n);
		} else {
			return Double.parseDouble(defaultQcFilter.get(key));
		}
	}

	public void setQcFilter(Map<String, String> qcFilter) {
		this.qcFilter = qcFilter;
	}

	public void setRange(String range) {
		this.range = range;
	}

	public String getRange() {
		return range;
	}

	public String getMapBeagle() {
		return mapBeagle;
	}

	public void setMapBeagle(String mapBeagle) {
		this.mapBeagle = mapBeagle;
	}

	public static RefPanel loadFromProperties(Object properties) throws IOException {
		if (properties != null) {
			RefPanel panel = new RefPanel();
			Map<String, Object> map = (Map<String, Object>) properties;

			if (map.get("hdfs") != null) {
				panel.setHdfs(map.get("hdfs").toString());
			} else {
				throw new IOException("Property 'hdfs' not found in cloudgene.yaml.");
			}

			if (map.get("id") != null) {
				panel.setId(map.get("id").toString());
			} else {
				throw new IOException("Property 'id' not found in cloudgene.yaml.");
			}

			if (map.get("legend") != null) {
				panel.setLegend(map.get("legend").toString());
			} else {
				throw new IOException("Property 'legend' not found in cloudgene.yaml.");
			}

			if (map.get("mapEagle") != null) {
				panel.setMapEagle(map.get("mapEagle").toString());
			}

			if (map.get("refEagle") != null) {
				panel.setRefEagle(map.get("refEagle").toString());
			}

			if (map.get("mapBeagle") != null) {
				panel.setMapBeagle(map.get("mapBeagle").toString());
			}

			if (map.get("refBeagle") != null) {
				panel.setRefBeagle(map.get("refBeagle").toString());
			}

			if (map.get("populations") != null) {
				panel.setPopulations((Map<String, String>) map.get("populations"));
			} else {
				throw new IOException("Property 'populations' not found in cloudgene.yaml.");
			}

			if (map.get("samples") != null) {
				panel.setSamples((Map<String, String>) map.get("samples"));
			} else {
				throw new IOException("Property 'samples' not found in cloudgene.yaml.");
			}

			if (map.get("qcFilter") != null) {
				panel.setQcFilter((Map<String, String>) map.get("qcFilter"));
			}

			// optional parameters
			if (map.get("build") != null) {
				panel.setBuild(map.get("build").toString());
			}

			if (map.get("range") != null) {
				panel.setRange(map.get("range").toString());
			}

			if (map.get("mapMinimac") != null) {
				panel.setMapMinimac(map.get("mapMinimac").toString());
			}

			return panel;

		} else {

			return null;
		}
	}
}
