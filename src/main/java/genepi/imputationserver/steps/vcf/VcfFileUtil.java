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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.broadinstitute.variant.vcf.VCFFileReader;

public class VcfFileUtil {

	public static VcfFile load(String vcfFilename, int chunksize,
			String tabixPath) throws IOException {

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

			// create index
			if (tabixPath != null) {

				Command tabix = new Command(tabixPath);

				tabix.setParams("-p", "vcf", vcfFilename);
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

	// TODO: Sebastian!!
	public static List<VcfFile> splitMaleFemale(VcfFile file, String tabix)
			throws IOException {
		List<VcfFile> files = new Vector<VcfFile>();
		// males
		VcfFile males = load(file.getVcfFilename(), file.getChunkSize(), tabix);
		Set<String> chromosomesMale = new HashSet<String>();
		chromosomesMale.add("X_male");
		males.setChromosomes(chromosomesMale);
		files.add(males);

		// females
		VcfFile females = load(file.getVcfFilename(), file.getChunkSize(),
				tabix);
		Set<String> chromosomesFemale = new HashSet<String>();
		chromosomesFemale.add("X_female");
		females.setChromosomes(chromosomesFemale);
		files.add(females);
		return files;
	}

	public static void splitChromosomeX(String inputFilename,
			String nonPseudoAutoFilename, String pseudoAutoFilename)
			throws IOException {

		LineWriter nonPseudoAuto = new LineWriter(nonPseudoAutoFilename);
		LineWriter pseudoAuto = new LineWriter(pseudoAutoFilename);

		LineReader reader = new LineReader(inputFilename);

		while (reader.next()) {

			String line = reader.get();

			if (line.startsWith("#")) {
				// header in booth files
				nonPseudoAuto.write(line);
				pseudoAuto.write(line);
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
					nonPseudoAuto.write(line);
				} else {
					pseudoAuto.write(line);
				}
			}

		}
		reader.close();
		pseudoAuto.close();
		nonPseudoAuto.close();

	}

	public static void extractNonPseudoAuto(String inputFilename,
			String nonPseudoAutoFilename) throws IOException {

		LineWriter nonPseudoAuto = new LineWriter(nonPseudoAutoFilename);

		LineReader reader = new LineReader(inputFilename);

		while (reader.next()) {

			String line = reader.get();

			if (line.startsWith("#")) {
				// header
				nonPseudoAuto.write(line);
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
					nonPseudoAuto.write(line);
				}
			}

		}
		reader.close();
		nonPseudoAuto.close();

	}

}
