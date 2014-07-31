package genepi.imputationserver.steps.vcf;

import genepi.hadoop.command.Command;
import genepi.io.FileUtil;
import genepi.io.text.LineReader;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

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

			while (lineReader.next()) {

				String line = lineReader.get();

				if (!line.startsWith("#")) {

					String tiles[] = line.split("\t", 6);

					if (tiles.length < 3) {
						throw new IOException(
								"Please use tab-delimited VCF files.");
					}

					String chromosome = tiles[0];
					int position = Integer.parseInt(tiles[1]);

					if (phased) {
						boolean containsSlash = tiles[5].contains("/");
						if (containsSlash) {
							phased = false;
						}

					}

					// TODO: check that all are phased
					// context.getGenotypes().get(0).isPhased();
					chromosomes.add(chromosome);
					if (chromosomes.size() > 1) {
						throw new IOException(
								"VCF file contains more than one chromosome. Please split your input vcf file by chromosome.");
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

}
