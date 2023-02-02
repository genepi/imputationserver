package genepi.imputationserver.steps.ancestry;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import genepi.imputationserver.steps.vcf.BGzipLineWriter;
import genepi.io.text.LineReader;

public class TraceInputValidation {

	public TraceInputValidationResult mergeAndCheckSites(String[] files, String referenceSites, String output)
			throws IOException {

		Map<String, String> sites = readReferenceSites(referenceSites);

		// TODO: check how vcf2geno filters: not in range --> set missing? how to check
		// different geno files between study and ref.

		// TODO: need sort?
		BGzipLineWriter writer = new BGzipLineWriter(output);
		boolean firstFile = true;
		TraceInputValidationResult result = new TraceInputValidationResult();
		for (String filename : files) {
			LineReader reader = new LineReader(filename);
			while (reader.next()) {
				String line = reader.get();
				if (line.startsWith("#")) {
					if (firstFile) {
						writer.write(line);
					}
				} else {

					String fields[] = line.split("\t");
					String alleles = sites.get(fields[0] + ":" + fields[1]);
					if (alleles != null) {
						if (alleles.equalsIgnoreCase(fields[3] + "/" + fields[4])) {
							writer.write(line);
							result.incFound();
						} else {
							if (alleles.equalsIgnoreCase(fields[4] + "/" + fields[3])) {
								// TODO: correct?
								result.incAlleleSwitches();
							} else {
								result.incAlleleMissmatch();
							}
						}
					} else {
						result.incNotFound();
					}
					result.incTotal();
				}

			}
			reader.close();
			firstFile = false;
		}
		writer.close();

		return result;

	}

	public Map<String, String> readReferenceSites(String filename) throws IOException {

		Map<String, String> sites = new HashMap<String, String>();

		BufferedReader reader = null;
		Pattern tabSeparatorPattern = null;

		String line = null;
		String[] fields = null;

		tabSeparatorPattern = Pattern.compile("\t");

		// reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new
		// FileInputStream(filename))));

		reader = new BufferedReader(new InputStreamReader(new FileInputStream(filename)));

		line = reader.readLine(); // skip header

		while ((line = reader.readLine()) != null) {
			fields = tabSeparatorPattern.split(line);
			sites.put(fields[0] + ":" + fields[1], fields[3] + "/" + fields[4]);
		}

		reader.close();

		return sites;

	}

	class TraceInputValidationResult {

		int total = 0;

		int found = 0;

		int alleleMissmatch = 0;

		int notFound = 0;

		int alleleSwitches = 0;

		public int getTotal() {
			return total;
		}

		public void setTotal(int total) {
			this.total = total;
		}

		public void incTotal() {
			this.total++;
		}

		public int getFound() {
			return found;
		}

		public void setFound(int found) {
			this.found = found;
		}

		public void incFound() {
			this.found++;
		}

		public int getAlleleMissmatch() {
			return alleleMissmatch;
		}

		public void setAlleleMissmatch(int alleleMissmatch) {
			this.alleleMissmatch = alleleMissmatch;
		}

		public void incAlleleMissmatch() {
			this.alleleMissmatch++;
		}

		public int getNotFound() {
			return notFound;
		}

		public void setNotFound(int notFound) {
			this.notFound = notFound;
		}

		public void incNotFound() {
			this.notFound++;
		}

		public int getAlleleSwitches() {
			return alleleSwitches;
		}

		public void setAlleleSwitches(int alleleSwitches) {
			this.alleleSwitches = alleleSwitches;
		}

		public void incAlleleSwitches() {
			this.alleleSwitches++;
		}

	}

}
