package genepi.imputationserver.util;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class RefPanel {

	private String id;

	private String pattern;

	private String hdfs;

	private String legend;

	private String legendPattern;

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
			status = FileSystem.get(new Configuration()).getFileStatus(
					new Path(hdfs));
			return new Date(status.getModificationTime()).toString() + " ("
					+ status.getLen() + " Bytes)";
		} catch (IOException e) {
			return "??";
		}

	}

	public void setHdfs(String hdfs) {
		this.hdfs = hdfs;
	}

	public String getPattern() {
		return pattern;
	}

	public void setPattern(String pattern) {
		this.pattern = pattern;
	}

	public String getLegend() {
		return legend;
	}

	public void setLegend(String legend) {
		this.legend = legend;
	}

	public String getLegendPattern() {
		return legendPattern;
	}

	public void setLegendPattern(String legendPattern) {
		this.legendPattern = legendPattern;
	}

}
