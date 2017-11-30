package genepi.imputationserver.util;

import java.io.IOException;
import java.util.Date;

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

	private String mapPatternShapeIT;

	private String mapPatternHapiUR;

	private String mapShapeIT;

	private String mapHapiUR;

	private String mapEagle;

	private String refEagle;

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

	public String getMapPatternShapeIT() {
		return mapPatternShapeIT;
	}

	public void setMapPatternShapeIT(String mapPatternShapeIT) {
		this.mapPatternShapeIT = mapPatternShapeIT;
	}

	public String getMapPatternHapiUR() {
		return mapPatternHapiUR;
	}

	public void setMapPatternHapiUR(String mapPatternHapiUR) {
		this.mapPatternHapiUR = mapPatternHapiUR;
	}

	public String getMapShapeIT() {
		return mapShapeIT;
	}

	public void setMapShapeIT(String mapShapeIT) {
		this.mapShapeIT = mapShapeIT;
	}

	public String getMapHapiUR() {
		return mapHapiUR;
	}

	public void setMapHapiUR(String mapHapiUR) {
		this.mapHapiUR = mapHapiUR;
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

	public boolean checkHapiUR() {

		if (mapHapiUR == null) {
			return false;
		}

		return HdfsUtil.exists(mapHapiUR);

	}

	public boolean checkShapeIT() {

		if (mapShapeIT == null) {
			return false;
		}

		return HdfsUtil.exists(mapShapeIT);

	}

}
