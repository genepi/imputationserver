package genepi.imputationserver.util;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import genepi.hadoop.HdfsUtil;

public class RefPanel {

	private String id;

	private String hdfs;

	private String legend;

	private String mapMinimac;

	private String build = "hg19";

	private String mapEagle;

	private String refEagle;

	private Map<String, String> samples;

	private Map<String, String> populations;

	private Map<String, String> qcFilter;

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

	public int getQcFilterByKey(String key) {
		if (qcFilter == null) {
			return 0;
		}
		String n = qcFilter.get(key);
		if (n != null) {
			return Integer.parseInt(n);
		} else {
			return 100;
		}
	}

	public void setQcFilter(Map<String, String> qcFilter) {
		this.qcFilter = qcFilter;
	}

}
