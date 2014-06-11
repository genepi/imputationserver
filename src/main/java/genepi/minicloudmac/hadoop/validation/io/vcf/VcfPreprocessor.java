package genepi.minicloudmac.hadoop.validation.io.vcf;

import genepi.hadoop.command.Command;
import genepi.io.FileUtil;
import genepi.io.text.LineReader;
import genepi.minicloudmac.hadoop.validation.io.AbstractPair;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.broadinstitute.variant.variantcontext.VariantContext;
import org.broadinstitute.variant.vcf.VCFFileReader;

public class VcfPreprocessor {
	private List<String> vcfFiles = new Vector<String>();

	private String error = null;

	private int chunksize;

	private String tabixPath;

	public VcfPreprocessor(int chunksize, String tabix) {
		this.chunksize = chunksize;
		this.tabixPath = tabix;
	}

	public VcfPair validate(String vcfFilename) {
		vcfFiles.add(vcfFilename);

		try {

			Set<Integer> chunks = new HashSet<Integer>();
			Set<String> chromosomes = new HashSet<String>();
			int noSnps = 0;
			int noSamples = 0;

			VCFFileReader reader = new VCFFileReader(new File(vcfFilename),
					false);

			noSamples = reader.getFileHeader().getGenotypeSamples().size();
			reader.close();

			LineReader lineReader = new LineReader(vcfFilename);

			boolean phased = true;

			while (lineReader.next()) {

				String line = lineReader.get();

				if (!line.startsWith("#")) {

					String tiles[] = line.split("\t", 3);
					String chromosome = tiles[0];
					int position = Integer.parseInt(tiles[1]);

					if (phased) {
						boolean containsSlash = tiles[2].contains("/");
						if (containsSlash) {
							phased = false;
						}

					}

					// TODO: check that all are phased
					// context.getGenotypes().get(0).isPhased();
					chromosomes.add(chromosome);
					if (chromosomes.size() > 1) {
						error = "Vcf file contains more than one chromosome. Please split your input vcf file by chromosome.";
						return null;
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

			/*
			 * TabixIndexCreator creator = new
			 * TabixIndexCreator(TabixFormat.VCF); creator.
			 */

			if (tabixPath != null) {

				Command tabix = new Command(tabixPath);

				tabix.setParams("-p", "vcf", vcfFilename);
				tabix.saveStdErr("tabix.output");
				int returnCode = tabix.execute();

				if (returnCode != 0) {
					error = "Error during index creation (" + returnCode
							+ "): " + FileUtil.readFileAsString("tabix.output");
					return null;
				}

			}

			VcfPair pair = new VcfPair();
			pair.setVcfFilename(vcfFilename);
			pair.setIndexFilename(vcfFilename + ".tbi");
			pair.setNoSnps(noSnps);
			pair.setNoSamples(noSamples);
			pair.setChunks(chunks);
			pair.setChromosomes(chromosomes);
			pair.setPhased(phased);
			return pair;

		} catch (Exception e) {

			error = e.getMessage();
			return null;

		}

	}

	public VcfPair validate2(String vcfFilename) {
		vcfFiles.add(vcfFilename);

		try {

			Set<Integer> chunks = new HashSet<Integer>();
			Set<String> chromosomes = new HashSet<String>();
			int noSnps = 0;
			int noSamples = 0;

			VCFFileReader reader = new VCFFileReader(new File(vcfFilename),
					false);

			noSamples = reader.getFileHeader().getGenotypeSamples().size();
			for (VariantContext context : reader) {

				// TODO: check that all are phased
				// context.getGenotypes().get(0).isPhased();
				chromosomes.add(context.getChr());
				if (chromosomes.size() > 1) {
					error = "Vcf file contains more than one chromosome. Please split your input vcf file by chromosome.";
					return null;
				}

				int chunk = context.getStart() / chunksize;
				if (context.getStart() % chunksize == 0) {
					chunk = chunk - 1;
				}
				chunks.add(chunk);
				noSnps++;

			}

			// create index

			// TabixIndexCreator creator = new
			// TabixIndexCreator(TabixFormat.VCF);
			// creator.finalizeIndex(0).

			reader.close();

			VcfPair pair = new VcfPair();
			pair.setVcfFilename(vcfFilename);
			pair.setIndexFilename(vcfFilename + ".idx");
			pair.setNoSnps(noSnps);
			pair.setNoSamples(noSamples);
			pair.setChunks(chunks);
			pair.setChromosomes(chromosomes);
			return pair;

		} catch (Exception e) {

			error = e.getMessage();
			return null;

		}

	}

	public String getError() {
		return error;
	}

	public static void main(String[] args) {

		int chunkSize = 2000000;

		VcfPreprocessor preprocessor = new VcfPreprocessor(chunkSize, "tabix");
		AbstractPair pair = preprocessor.validate("test-data/l.vcf.gz");

		int chunks = 0;

		int noSnps = 0;
		int noSamples = 0;
		Set<String> chromosomes = new HashSet<String>();

		noSamples = pair.getNoSamples();
		noSnps += pair.getNoSnps();
		chromosomes.addAll(pair.getChromosomes());

		for (int chunk : pair.getChunks()) {
			chunks++;
		}

		String chromosomeString = "";
		for (String chr : chromosomes) {
			chromosomeString += " " + chr;
		}

		System.out.println("Samples: " + noSamples + "\n" + "Chromosomes:"
				+ chromosomeString + "\n" + "SNPs: " + noSnps + "\n"
				+ "Chunks: " + chunks + "\n" + "Datatype: "
				// + (folderLoad.isPhasedFiles() ? "phased" : "unphased")
				+ ("unphased") + "\n" + "Genome Build: 37");

	}

}
