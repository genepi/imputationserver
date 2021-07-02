package genepi.imputationserver.util;

import java.util.ArrayList;

public class ExportObject {

	private ArrayList<String> dataFiles;
	private ArrayList<String> dataMetaFiles;
	private ArrayList<String> headerFiles;
	private ArrayList<String> headerMetaFiles;
	private ArrayList<String> infoFiles;

	public ExportObject() {
		dataFiles = new ArrayList<String>();
		headerFiles = new ArrayList<String>();
		infoFiles = new ArrayList<String>();
		headerMetaFiles = new ArrayList<String>();
		dataMetaFiles = new ArrayList<String>();
	}

	public ArrayList<String> getDataMetaFiles() {
		return dataMetaFiles;
	}

	public void setDataMetaFiles(ArrayList<String> dataMetaFiles) {
		this.dataMetaFiles = dataMetaFiles;
	}

	public ArrayList<String> getHeaderMetaFiles() {
		return headerMetaFiles;
	}

	public void setHeaderMetaFiles(ArrayList<String> headerMetaFiles) {
		this.headerMetaFiles = headerMetaFiles;
	}

	public ArrayList<String> getDataFiles() {
		return dataFiles;
	}

	public void setDataFiles(ArrayList<String> dataFiles) {
		this.dataFiles = dataFiles;
	}

	public ArrayList<String> getHeaderFiles() {
		return headerFiles;
	}

	public void setHeaderFiles(ArrayList<String> headerFiles) {
		this.headerFiles = headerFiles;
	}

	public ArrayList<String> getInfoFiles() {
		return infoFiles;
	}

	public void setInfoFiles(ArrayList<String> infoFiles) {
		this.infoFiles = infoFiles;
	}
}
