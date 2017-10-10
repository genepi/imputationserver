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

	private String build = "hg19";
	
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
			status = FileSystem.get(HdfsUtil.getConfiguration()).getFileStatus(
					new Path(hdfs));
			return new Date(status.getModificationTime()).toString() + " ("
					+ status.getLen() + " Bytes)";
		} catch (IOException e) {
			return "??";
		}

	}

	public boolean existsReference() {

		try {
			return FileSystem.get(HdfsUtil.getConfiguration()).exists(new Path(hdfs));
		} catch (IOException e) {
			return false;
		}

	}
	
	public boolean existsLegend() {

		try {
			return FileSystem.get(HdfsUtil.getConfiguration()).exists(new Path(hdfs));
		} catch (IOException e) {
			return false;
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

}
