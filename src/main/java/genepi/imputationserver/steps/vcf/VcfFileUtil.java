package genepi.imputationserver.steps.vcf;

import genepi.hadoop.command.Command;
import genepi.io.FileUtil;
import genepi.io.text.LineReader;
import genepi.io.text.LineWriter;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.broadinstitute.variant.vcf.VCFFileReader;

public class VcfFileUtil {

	public static String BINARIES = "bin/";

	public static void setBinaries(String binaries) {
		BINARIES = binaries;
	}

	public static VcfFile load(String vcfFilename, int chunksize,
			boolean createIndex) throws IOException {

		Set<Integer> chunks = new HashSet<Integer>();
		Set<String> chromosomes = new HashSet<String>();
		int noSnps = 0;
		int noSamples = 0;

		try {

			VCFFileReader reader = new VCFFileReader(new File(vcfFilename),
					false);

			noSamples = reader.getFileHeader().getGenotypeSamples().size();

			reader.close();

			LineReader lineReader = new LineReader(vcfFilename);

			boolean phased = true;
			boolean phasedAutodetect = true;
			boolean firstLine = true;

			while (lineReader.next()) {

				String line = lineReader.get();

				if (!line.startsWith("#")) {

					String tiles[] = line.split("\t", 10);

					if (tiles.length < 3) {
						throw new IOException(
								"The provided VCF file is not tab-delimited");
					}

					String chromosome = tiles[0];
					int position = Integer.parseInt(tiles[1]);

					if (phased) {
						boolean containsSymbol = tiles[9].contains("/");

						if (containsSymbol) {
							phased = false;
						}

					}

					if (firstLine) {
						boolean containsSymbol = tiles[9].contains("/")
								|| tiles[9].contains(".");

						if (!containsSymbol) {
							phasedAutodetect = true;
						} else {
							phasedAutodetect = false;
						}
						firstLine = false;

					}

					// TODO: check that all are phased
					// context.getGenotypes().get(0).isPhased();
					chromosomes.add(chromosome);
					if (chromosomes.size() > 1) {
						throw new IOException(
								"The provided VCF file contains more than one chromosome. Please split your input VCF file by chromosome");
					}

					String ref = tiles[3];
					String alt = tiles[4];

					if (ref.equals(alt)) {
						throw new IOException(
								"The provided VCF file is malformed at variation "
										+ tiles[2] + ": reference allele ("
										+ ref + ") and alternate allele  ("
										+ alt + ") are the same.");
					}

					int chunk = position / chunksize;
					if (position % chunksize == 0) {
						chunk = chunk - 1;
					}
					chunks.add(chunk);
					noSnps++;

				} else {

					if (line.startsWith("#CHROM")) {

						String[] tiles = line.split("\t");

						// check sample names, stop when not unique
						HashSet<String> samples = new HashSet<>();

						for (int i = 0; i < tiles.length; i++) {

							String sample = tiles[i];

							if (samples.contains(sample)) {
								reader.close();
								throw new IOException(
										"Two individuals or more have the following ID: "
												+ sample);
							}
							samples.add(sample);
						}
					}

				}

			}
			lineReader.close();
			reader.close();

			String tabixPath = FileUtil.path(BINARIES, "tabix");

			// create index
			if (new File(tabixPath).exists() && createIndex) {

				Command tabix = new Command(tabixPath);
				tabix.setParams("-f", "-p", "vcf", vcfFilename);
				tabix.saveStdErr("tabix.output");
				int returnCode = tabix.execute();

				if (returnCode != 0) {
					throw new IOException(
							"The provided VCF file is malformed. Error during index creation: "
									+ FileUtil.readFileAsString("tabix.output"));
				}

			}

			VcfFile pair = new VcfFile();
			pair.setVcfFilename(vcfFilename);
			pair.setIndexFilename(vcfFilename + ".tbi");
			pair.setNoSnps(noSnps);
			pair.setNoSamples(noSamples);
			pair.setChunks(chunks);
			pair.setChromosomes(chromosomes);
			pair.setPhased(phased);
			pair.setPhasedAutodetect(phasedAutodetect);
			pair.setChunkSize(chunksize);
			return pair;

		} catch (Exception e) {
			throw new IOException(e.getMessage());
		}

	}

	public static Set<String> validChromosomes = new HashSet<String>();

	static {

		validChromosomes.add("1");
		validChromosomes.add("2");
		validChromosomes.add("3");
		validChromosomes.add("4");
		validChromosomes.add("5");
		validChromosomes.add("6");
		validChromosomes.add("7");
		validChromosomes.add("8");
		validChromosomes.add("9");
		validChromosomes.add("10");
		validChromosomes.add("11");
		validChromosomes.add("12");
		validChromosomes.add("13");
		validChromosomes.add("14");
		validChromosomes.add("15");
		validChromosomes.add("16");
		validChromosomes.add("17");
		validChromosomes.add("18");
		validChromosomes.add("19");
		validChromosomes.add("20");
		validChromosomes.add("21");
		validChromosomes.add("22");
	}

	public static boolean isAutosomal(String chromosome) {
		return validChromosomes.contains(chromosome);
	}

	public static void mergeGz(String local, String hdfs, String ext)
			throws FileNotFoundException, IOException {
		GZIPOutputStream out = new GZIPOutputStream(new FileOutputStream(local));
		merge(out, hdfs, ext);
	}

	public static void merge(OutputStream out, String hdfs, String ext)
			throws IOException {

		Configuration conf = new Configuration();

		FileSystem fileSystem = FileSystem.get(conf);
		Path pathFolder = new Path(hdfs);
		FileStatus[] files = fileSystem.listStatus(pathFolder);

		List<String> filenames = new Vector<String>();

		if (files != null) {

			// filters by extension and sorts by filename
			for (FileStatus file : files) {
				if (!file.isDir()
						&& !file.getPath().getName().startsWith("_")
						&& (ext == null || file.getPath().getName()
								.endsWith(ext))) {
					filenames.add(file.getPath().toString());
				}
			}
			Collections.sort(filenames);

			boolean firstFile = true;
			boolean firstLine = true;

			for (String filename : filenames) {

				Path path = new Path(filename);

				FSDataInputStream in = fileSystem.open(path);

				LineReader reader = new LineReader(in);

				while (reader.next()) {

					String line = reader.get();

					if (line.startsWith("#")) {

						if (firstFile) {
							if (!firstLine) {
								out.write('\n');
							}
							out.write(line.getBytes());
							firstLine = false;
						}

					} else {

						if (!firstLine) {
							out.write('\n');
						}
						out.write(line.getBytes());
						firstLine = false;
					}

				}

				in.close();
				firstFile = false;

			}

			out.close();
		}

	}

	public static List<VcfFile> prepareChrX(VcfFile file) throws IOException {
		List<VcfFile> files = new Vector<VcfFile>();

		List<String> b1 = new Vector<String>();
		List<String> b2 = new Vector<String>();

		String VCFKEEPSAMPLES = FileUtil.path(BINARIES, "vcfkeepsamples");
		String PLINK = FileUtil.path(BINARIES, "plink");
		String TABIX = FileUtil.path(BINARIES, "tabix");
		String BGZIP = FileUtil.path(BINARIES, "bgzip");

		if (!new File(VCFKEEPSAMPLES).exists()) {
			throw new IOException("vcfkeepsamples: file " + VCFKEEPSAMPLES
					+ " not found.");
		}

		if (!new File(PLINK).exists()) {
			throw new IOException("plink: file " + PLINK + " not found.");
		}

		if (!new File(TABIX).exists()) {
			throw new IOException("tabix: file " + TABIX + " not found.");
		}

		if (!new File(BGZIP).exists()) {
			throw new IOException("bgzip: file " + BGZIP + " not found.");
		}

		/** Split file into no.auto (or nonpar) and auto (or par) region */
		VcfFileUtil.splitFileByRegion(file.getVcfFilename());

		// bgzip
		Command bgzip3 = new Command(BGZIP);
		bgzip3.setSilent(true);
		bgzip3.setParams(file.getVcfFilename() + ".no.auto.vcf");
		System.out.println("Command: " + bgzip3.getExecutedCommand());
		
		if (bgzip3.execute() != 0) {
			throw new IOException(
					"Something went wrong with the bgzip no.auto command");
		}

		// tabix
		Command tabix2 = new Command(TABIX);
		tabix2.setSilent(true);
		tabix2.setParams("-f", file.getVcfFilename() + ".no.auto.vcf.gz");
		System.out.println("Command: " + tabix2.getExecutedCommand());

		
		if (tabix2.execute() != 0) {
			throw new IOException(
					"Something went wrong with the tabix no.auto command");
		}

		Command sexCheck = new Command(PLINK);
		sexCheck.setSilent(false);
		sexCheck.setParams("--vcf", file.getVcfFilename() + ".no.auto.vcf.gz",
				"--check-sex", "--const-fid", "--out", file.getVcfFilename());
		System.out.println("Command: " + sexCheck.getExecutedCommand());

		if (sexCheck.execute() != 0) {
			throw new IOException("Something went wrong with the plink command");
		}

		LineReader lr = new LineReader(file.getVcfFilename() + ".sexcheck");
		while (lr.next()) {
			String[] a = lr.get().split("\\s+");

			if (a[4].equals("1")) {
				b1.add(a[2]);
			} else if (a[4].equals("2")) {
				b2.add(a[2]);
			}

		}

		/** Write for nonpar region a seperate file for male and female */
		Command keepSamples = new Command(VCFKEEPSAMPLES);
		keepSamples.setSilent(true);

		String[] params = new String[b1.size() + 1];
		params[0] = file.getVcfFilename() + ".no.auto.vcf.gz";
		for (int i = 0; i < b1.size(); i++) {
			params[i + 1] = b1.get(i);
		}
		keepSamples.setParams(params);
		System.out.println("Command: " + keepSamples.getExecutedCommand());
		keepSamples.saveStdOut(file.getVcfFilename() + "-m.vcf");

		if (keepSamples.execute() != 0) {
			throw new IOException(
					"Something went wrong with the keepSamples male command");
		}

		// bgzip
		Command bgzip = new Command(BGZIP);
		bgzip.setSilent(true);
		bgzip.setParams(file.getVcfFilename() + "-m.vcf");

		if (bgzip.execute() != 0) {
			throw new IOException(
					"Something went wrong with the bgzip male command");
		}
		params = new String[b2.size() + 1];
		params[0] = file.getVcfFilename() + ".no.auto.vcf.gz";
		for (int i = 0; i < b2.size(); i++) {
			params[i + 1] = b2.get(i);
		}
		keepSamples.setSilent(true);

		keepSamples.setParams(params);
		System.out.println("Command: " + keepSamples.getExecutedCommand());
		keepSamples.saveStdOut(file.getVcfFilename() + "-f.vcf");

		if (keepSamples.execute() != 0) {
			throw new IOException(
					"Something went wrong with the kepsample female command");
		}

		// bgzip
		Command bgzip2 = new Command(BGZIP);
		bgzip2.setSilent(true);
		bgzip2.setParams(file.getVcfFilename() + "-f.vcf");

		if (bgzip2.execute() != 0) {
			throw new IOException(
					"Something went wrong with the bgzip female command");
		}

		/** males-nopar */
		VcfFile males = load(file.getVcfFilename() + "-m.vcf.gz",
				file.getChunkSize(), true);
		Set<String> chromosomesMale = new HashSet<String>();
		chromosomesMale.add("X.no.auto_male");
		males.setChromosomes(chromosomesMale);
		files.add(males);

		/** females-nopar */
		VcfFile females = load(file.getVcfFilename() + "-f.vcf.gz",
				file.getChunkSize(), true);

		Set<String> chromosomesFemale = new HashSet<String>();
		chromosomesFemale.add("X.no.auto_female");
		females.setChromosomes(chromosomesFemale);
		files.add(females);

		/** par, no SEX split */
		// bgzip
		bgzip3 = new Command(BGZIP);
		bgzip3.setSilent(true);
		bgzip3.setParams(file.getVcfFilename() + ".auto.vcf");

		if (bgzip3.execute() != 0) {
			throw new IOException(
					"Something went wrong with the bgzip auto command");
		}

		VcfFile par = load(file.getVcfFilename() + ".auto.vcf.gz",
				file.getChunkSize(), true);
		Set<String> chromosomesXPar = new HashSet<String>();
		chromosomesXPar.add("X.auto");
		par.setChromosomes(chromosomesXPar);
		files.add(par);

		return files;
	}

	public static void splitFileByRegion(String inputFilename)
			throws IOException {

		LineWriter nonPar = new LineWriter(inputFilename + ".no.auto.vcf");

		LineWriter par = new LineWriter(inputFilename + ".auto.vcf");

		LineReader reader = new LineReader(inputFilename);

		while (reader.next()) {

			String line = reader.get();

			if (line.startsWith("#")) {
				// header
				nonPar.write(line);
				par.write(line);
			} else {
				String tiles[] = line.split("\t", 3);

				if (tiles.length < 3) {
					throw new IOException(
							"The provided VCF file is not tab-delimited");
				}

				String chromosome = tiles[0];

				if (!chromosome.equals("X")) {
					throw new IOException(
							"The provided VCF file is not for chromosome X");
				}

				int position = Integer.parseInt(tiles[1]);

				if (2699520 <= position && position <= 154931044) {
					nonPar.write(line);
				} else {
					par.write(line);
				}
			}

		}
		reader.close();
		nonPar.close();
		par.close();

	}

	/*
	 * public static void splitChromosomeX(String inputFilename, String
	 * nonPseudoAutoFilename, String pseudoAutoFilename) throws IOException {
	 * 
	 * LineWriter nonPseudoAuto = new LineWriter(nonPseudoAutoFilename);
	 * LineWriter pseudoAuto = new LineWriter(pseudoAutoFilename);
	 * 
	 * LineReader reader = new LineReader(inputFilename);
	 * 
	 * while (reader.next()) {
	 * 
	 * String line = reader.get();
	 * 
	 * if (line.startsWith("#")) { // header in both files
	 * nonPseudoAuto.write(line); pseudoAuto.write(line); } else { String
	 * tiles[] = line.split("\t", 3);
	 * 
	 * if (tiles.length < 3) { throw new IOException(
	 * "The provided VCF file is not tab-delimited"); }
	 * 
	 * String chromosome = tiles[0];
	 * 
	 * if (!chromosome.equals("X")) { throw new IOException(
	 * "The provided VCF file is not for chromosome X"); }
	 * 
	 * int position = Integer.parseInt(tiles[1]);
	 * 
	 * if (2699520 <= position && position <= 154931044) {
	 * nonPseudoAuto.write(line); } else { pseudoAuto.write(line); } }
	 * 
	 * } reader.close(); pseudoAuto.close(); nonPseudoAuto.close();
	 * 
	 * }
	 */

	/*
	 * public static void extractNonPseudoAuto(String inputFilename, String
	 * nonPseudoAutoFilename) throws IOException {
	 * 
	 * LineWriter nonPseudoAuto = new LineWriter(nonPseudoAutoFilename);
	 * 
	 * LineReader reader = new LineReader(inputFilename);
	 * 
	 * while (reader.next()) {
	 * 
	 * String line = reader.get();
	 * 
	 * if (line.startsWith("#")) { // header nonPseudoAuto.write(line); } else {
	 * String tiles[] = line.split("\t", 3);
	 * 
	 * if (tiles.length < 3) { throw new IOException(
	 * "The provided VCF file is not tab-delimited"); }
	 * 
	 * String chromosome = tiles[0];
	 * 
	 * if (!chromosome.equals("X")) { throw new IOException(
	 * "The provided VCF file is not for chromosome X"); }
	 * 
	 * int position = Integer.parseInt(tiles[1]);
	 * 
	 * if (2699520 <= position && position <= 154931044) {
	 * nonPseudoAuto.write(line); } }
	 * 
	 * } reader.close(); nonPseudoAuto.close();
	 * 
	 * }
	 */

	/*
	 * public static void extractPseudoAuto(String inputFilename, String
	 * nonPseudoAutoFilename) throws IOException {
	 * 
	 * LineWriter pseudoAuto = new LineWriter(nonPseudoAutoFilename);
	 * 
	 * LineReader reader = new LineReader(inputFilename);
	 * 
	 * while (reader.next()) {
	 * 
	 * String line = reader.get();
	 * 
	 * if (line.startsWith("#")) { // header pseudoAuto.write(line); } else {
	 * String tiles[] = line.split("\t", 3);
	 * 
	 * if (tiles.length < 3) { throw new IOException(
	 * "The provided VCF file is not tab-delimited"); }
	 * 
	 * String chromosome = tiles[0];
	 * 
	 * if (!chromosome.equals("X")) { throw new IOException(
	 * "The provided VCF file is not for chromosome X"); }
	 * 
	 * int position = Integer.parseInt(tiles[1]);
	 * 
	 * if (!(2699520 <= position && position <= 154931044)) {
	 * pseudoAuto.write(line); } }
	 * 
	 * } reader.close(); pseudoAuto.close();
	 * 
	 * }
	 */

}
