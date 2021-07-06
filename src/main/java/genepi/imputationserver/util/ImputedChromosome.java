package genepi.imputationserver.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

public class ImputedChromosome {

	private List<String> dataFiles;

	private List<String> dataMetaFiles;

	private List<String> headerFiles;

	private List<String> headerMetaFiles;

	private List<String> infoFiles;

	public ImputedChromosome() {
		dataFiles = new Vector<String>();
		headerFiles = new Vector<String>();
		infoFiles = new Vector<String>();
		headerMetaFiles = new ArrayList<String>();
		dataMetaFiles = new Vector<String>();
	}

	public List<String> getDataMetaFiles() {
		return dataMetaFiles;
	}

	public void addDataMetaFiles(List<String> dataMetaFiles) {
		this.dataMetaFiles.addAll(dataMetaFiles);
	}

	public List<String> getHeaderMetaFiles() {
		return headerMetaFiles;
	}

	public void addHeaderMetaFiles(List<String> headerMetaFiles) {
		this.headerMetaFiles.addAll(headerMetaFiles);
	}

	public List<String> getDataFiles() {
		return dataFiles;
	}

	public void addDataFiles(List<String> dataFiles) {
		this.dataFiles.addAll(dataFiles);
	}

	public List<String> getHeaderFiles() {
		return headerFiles;
	}

	public void addHeaderFiles(List<String> headerFiles) {
		this.headerFiles.addAll(headerFiles);
	}

	public List<String> getInfoFiles() {
		return infoFiles;
	}

	public void addInfoFiles(List<String> infoFiles) {
		this.infoFiles.addAll(infoFiles);
	}
}
