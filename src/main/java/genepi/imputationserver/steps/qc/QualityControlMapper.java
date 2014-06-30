package genepi.imputationserver.steps.qc;

import genepi.hadoop.CacheStore;
import genepi.hadoop.HdfsUtil;
import genepi.hadoop.ParameterStore;
import genepi.hadoop.PreferenceStore;
import genepi.hadoop.io.HdfsLineWriter;
import genepi.imputationserver.steps.vcf.VcfChunk;
import genepi.io.FileUtil;
import genepi.io.legend.LegendEntry;
import genepi.io.legend.LegendFileReader;
import genepi.io.text.LineReader;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.broadinstitute.variant.variantcontext.VariantContext;
import org.broadinstitute.variant.vcf.VCFCodec;
import org.broadinstitute.variant.vcf.VCFFileReader;
import org.broadinstitute.variant.vcf.VCFHeaderVersion;

public class QualityControlMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	private String folder;

	private LegendFileReader legendReader;

	private String oldChromosome = "";

	private String legendPattern;

	private String legendFile;

	private String population;

	private String output;

	private String outputRemovedSnps;

	private int lastPos = 0;

	private int phasingWindow;

	protected void setup(Context context) throws IOException,
			InterruptedException {

		// read parameters
		ParameterStore parameters = new ParameterStore(context);
		legendPattern = parameters.get(QualityControlJob.LEGEND_PATTERN);
		population = parameters.get(QualityControlJob.LEGEND_POPULATION);
		output = parameters.get(QualityControlJob.OUTPUT_MAF);
		outputRemovedSnps = parameters
				.get(QualityControlJob.OUTPUT_REMOVED_SNPS);
		String hdfsPath = parameters.get(QualityControlJob.LEGEND_HDFS);
		String legendFilename = FileUtil.getFilename(hdfsPath);

		// load files from cache
		CacheStore cache = new CacheStore(context.getConfiguration());
		legendFile = cache.getArchive(legendFilename);

		// create temp directory
		PreferenceStore store = new PreferenceStore(context.getConfiguration());
		folder = store.getString("minimac.tmp");
		folder = FileUtil.path(folder, context.getTaskAttemptID().toString());
		FileUtil.createDirectory(folder);
		phasingWindow = Integer.parseInt(store.getString("phasing.window"));
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {

		// delete temp directory
		FileUtil.deleteDirectory(folder);

	}

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		if (value.toString() == null || value.toString().isEmpty()) {
			return;
		}

		VcfChunk chunk = new VcfChunk(value.toString());
		String vcfFilename = FileUtil.path(folder, "minimac.vcf.gz");
		String vcfFilenameIndex = FileUtil.path(folder, "minimac.vcf.gz.tbi");

		HdfsUtil.get(chunk.getVcfFilename(), vcfFilename);
		HdfsUtil.get(chunk.getIndexFilename(), vcfFilenameIndex);

		// int errors = 0;

		HdfsLineWriter statisticWriter = new HdfsLineWriter(HdfsUtil.path(
				output, chunk.toString()));

		HdfsLineWriter logWriter = new HdfsLineWriter(HdfsUtil.path(
				outputRemovedSnps, chunk.toString()));

		HdfsLineWriter chunkWriter = new HdfsLineWriter(HdfsUtil.path(
				outputRemovedSnps, "exlcude" + chunk.toString()));

		String hdfsFilename = chunk.getVcfFilename() + "_" + chunk.getId();

		HdfsLineWriter newFileWriter = new HdfsLineWriter(hdfsFilename);
		// +/- phasingWindow (1 Mbases default)
		int start = chunk.getStart() - phasingWindow;
		if (start < 1) {
			start = 1;
		}

		int end = chunk.getEnd() + phasingWindow;

		LineReader reader = new LineReader(vcfFilename);
		VCFFileReader vcfReader = new VCFFileReader(new File(vcfFilename));
		VCFCodec codec = new VCFCodec();

		codec.setVCFHeader(vcfReader.getFileHeader(), VCFHeaderVersion.VCF4_1);

		int foundInLegend = 0;
		int notFoundInLegend = 0;
		int alleleMismatch = 0;
		int lowCallRate = 0;
		int filtered = 0;
		int removedChunks = 0;
		int overallSnps = 0;
		int monomorphic = 0;
		int alternativeAlleles = 0;
		int noSnps = 0;
		int duplicates = 0;
		int filterFlag = 0;
		int invalidAlleles = 0;

		int[] snpsPerSampleCount = null;

		while (reader.next()) {

			String line = reader.get();

			if (line.startsWith("#")) {

				newFileWriter.write(line);

			} else {

				String tiles[] = line.split("\t", 6);
				int position = Integer.parseInt(tiles[1]);
				String ref = tiles[3];
				String alt = tiles[4];

				boolean insideChunk = position >= chunk.getStart()
						&& position <= chunk.getEnd();

				// filter invalid alleles
				if (!isValid(ref) || !isValid(alt)) {
					if (insideChunk) {
						logWriter.write("Invalid Alleles: " + tiles[0] + " ("
								+ ref + "/" + alt + ")");
						invalidAlleles++;
						filtered++;
					}
					continue;
				}

				VariantContext snp = codec.decode(line);

				// count duplicates
				if ((lastPos == snp.getStart() && lastPos > 0)) {
					if (insideChunk) {
						duplicates++;
						logWriter.write("Duplicate: " + snp.getID());
						filtered++;
					}
					lastPos = snp.getStart();
					continue;

				}

				// update lastpos only when not filtered
				if (!snp.isFiltered()) {
					lastPos = snp.getStart();
				}

				if (snpsPerSampleCount == null) {
					snpsPerSampleCount = new int[snp.getNSamples()];
					for (int i = 0; i < snp.getNSamples(); i++) {
						snpsPerSampleCount[i] = 0;
					}
				}

				// filter flag
				if (snp.isFiltered()) {
					if (insideChunk) {

						if (snp.getFilters().contains("DUP")) {
							duplicates++;
							logWriter.write("Duplicate: " + snp.getID());
							filtered++;
						} else {

							logWriter.write("Filter-Flag set: " + snp.getID());
							filterFlag++;
							filtered++;
						}
					}
					continue;
				}

				// alternative allele frequency
				int hetVarOnes = snp.getHetCount();
				int homVarOnes = snp.getHomVarCount() * 2;
				double af = (double) ((hetVarOnes + homVarOnes) / (double) (((snp
						.getNSamples() - snp.getNoCallCount()) * 2)));
				if (af > 0.5) {
					if (insideChunk) {
						alternativeAlleles++;
					}
				}

				// filter indels
				if (snp.isIndel() || snp.isComplexIndel()) {
					if (insideChunk) {
						logWriter.write("InDel: " + snp.getID());
						noSnps++;
						filtered++;
					}
					continue;
				}

				// remove monomorphic snps
				if (snp.isMonomorphicInSamples()) {
					if (insideChunk) {
						// System.out.println(snp.getChr()+":"+snp.getStart());
						logWriter.write("Monomorphic: " + snp.getID());
						monomorphic++;
						filtered++;
					}
					continue;
				}

				LegendEntry refSnp = getReader(snp.getChr()).findByPosition2(
						snp.getStart());

				// not found in legend file
				if (refSnp == null) {
					// write to vcf file
					newFileWriter.write(line);
					if (insideChunk) {
						overallSnps++;
						notFoundInLegend++;
					}
					continue;
				} else {

					if (insideChunk) {
						foundInLegend++;
					}

					char legendRef = refSnp.getAlleleA();
					char legendAlt = refSnp.getAlleleB();
					char studyRef = snp.getReference().getBaseString()
							.charAt(0);
					char studyAlt = snp.getAltAlleleWithHighestAlleleCount()
							.getBaseString().charAt(0);

					// filter aleele mismatches
					if (legendRef != studyRef || studyAlt != legendAlt) {
						if (insideChunk) {
							logWriter.write("Allele mismatch: " + snp.getID()
									+ " (ref: " + legendRef + "/" + legendAlt
									+ ", data: " + studyRef + "/" + studyAlt
									+ ")");
							alleleMismatch++;
							filtered++;
						}
						continue;
					}

					// filter low call rate
					if (snp.getNoCallCount() / (double) snp.getNSamples() > 0.10) {
						if (insideChunk) {
							logWriter
									.write("Low call rate: "
											+ snp.getID()
											+ " ("
											+ (1.0 - snp.getNoCallCount()
													/ (double) snp
															.getNSamples())
											+ ")");
							lowCallRate++;
							filtered++;
						}
						continue;
					}

					// allele-frequency check
					if (insideChunk) {
						if (!population.equals("mixed")) {

							SnpStats statistics = calculateAlleleFreq(snp,
									refSnp);
							statisticWriter.write(snp.getID() + "\t"
									+ statistics.toString());
						}
						overallSnps++;
					}

					// write only SNPs into minimac file
					// which came up to this point
					if (position >= start && position <= end) {

						newFileWriter.write(line);

						// check if all samples have
						// enough SNPs
						if (insideChunk) {
							int i = 0;
							for (String sample : snp
									.getSampleNamesOrderedByName()) {
								if (snp.getGenotype(sample).isCalled()) {
									snpsPerSampleCount[i] += 1;
								}
								i++;
							}
						}
					}

				}

			}
		}
		newFileWriter.close();

		// this checks if enough SNPs are included in each sample
		boolean acceptChunk = true;
		for (int i = 0; i < snpsPerSampleCount.length; i++) {
			int snps = snpsPerSampleCount[i];
			if (snps / (double) overallSnps < 0.8) {
				acceptChunk = false;
				chunkWriter.write(chunk.toString()
						+ " Sample "
						+ vcfReader.getFileHeader().getSampleNamesInOrder()
								.get(i) + ": call rate: "
						+ (snps / (double) overallSnps));
			}

		}

		// this checks if the amount of not found SNPs in the reference panel is
		// smaller than 50 %. At least 3 SNPs must be included in each chunk

		double overlap = foundInLegend
				/ (double) (foundInLegend + notFoundInLegend);

		if (overlap >= 0.5 && overallSnps >= 3 && acceptChunk) {

			// update chunk
			chunk.setSnps(overallSnps);
			chunk.setInReference(foundInLegend);
			chunk.setVcfFilename(hdfsFilename);
			context.write(new Text(chunk.getChromosome()),
					new Text(chunk.serialize()));
		} else {
			chunkWriter.write(chunk.toString() + " (Snps: " + overallSnps
					+ ", Reference overlap: " + overlap
					+ ", low sample call rates: " + !acceptChunk + ")");
			removedChunks++;
		}

		vcfReader.close();
		reader.close();
		legendReader.close();

		statisticWriter.write("");
		statisticWriter.close();

		logWriter.write("");
		logWriter.close();

		chunkWriter.write("");
		chunkWriter.close();

		context.getCounter("minimac", "alternativeAlleles").increment(
				alternativeAlleles);
		context.getCounter("minimac", "monomorphic").increment(monomorphic);
		context.getCounter("minimac", "noSnps").increment(noSnps);
		context.getCounter("minimac", "duplicates").increment(duplicates);
		context.getCounter("minimac", "foundInLegend").increment(foundInLegend);
		context.getCounter("minimac", "notFoundInLegend").increment(
				notFoundInLegend);
		context.getCounter("minimac", "alleleMismatch").increment(
				alleleMismatch);
		context.getCounter("minimac", "toLessSamples").increment(lowCallRate);
		context.getCounter("minimac", "filtered").increment(filtered);
		context.getCounter("minimac", "removedChunks").increment(removedChunks);
		context.getCounter("minimac", "filterFlag").increment(filterFlag);
		context.getCounter("minimac", "invalidAlleles").increment(
				invalidAlleles);
		context.getCounter("minimac", "remainingSnps").increment(overallSnps);
		// write updated value out

	}

	private SnpStats calculateAlleleFreq(VariantContext snp, LegendEntry refSnp)
			throws IOException, InterruptedException {

		// calculate allele frequency
		SnpStats output = new SnpStats();

		int position = snp.getStart();

		double chisq = 0;

		// TODO 1000G REF 1 samples count
		int refN = 1092 * 2;

		double refA = refSnp.getFrequencyA();
		double refB = refSnp.getFrequencyB();

		int countRef = snp.getHetCount() + snp.getHomRefCount() * 2;
		int countAlt = snp.getHetCount() + snp.getHomVarCount() * 2;

		double p = countRef / (double) (countRef + countAlt);
		double q = countAlt / (double) (countRef + countAlt);
		double studyN = (snp.getNSamples() - snp.getNoCallCount()) * 2;

		double totalQ = q * studyN + refB * refN;
		double expectedQ = totalQ / (studyN + refN) * studyN;
		double deltaQ = q * studyN - expectedQ;

		chisq += (Math.pow(deltaQ, 2) / expectedQ)
				+ (Math.pow(deltaQ, 2) / (totalQ - expectedQ));

		double totalP = p * studyN + refA * refN;
		double expectedP = totalP / (studyN + refN) * studyN;
		double deltaP = p * studyN - expectedP;

		chisq += (Math.pow(deltaP, 2) / expectedP)
				+ (Math.pow(deltaP, 2) / (totalP - expectedP));

		output.setType("SNP");
		output.setPosition(position);
		output.setChromosome(snp.getChr());
		output.setRefFrequencyA(refSnp.getFrequencyA());
		output.setRefFrequencyB(refSnp.getFrequencyB());
		output.setFrequencyA((float) p);
		output.setFrequencyB((float) q);
		output.setChisq(chisq);
		output.setAlleleA(snp.getReference().getBaseString().charAt(0));
		output.setAlleleB(snp.getAltAlleleWithHighestAlleleCount()
				.getBaseString().charAt(0));
		output.setRefAlleleA(refSnp.getAlleleA());
		output.setRefAlleleB(refSnp.getAlleleB());
		output.setOverlapWithReference(true);

		return output;
	}

	private LegendFileReader getReader(String chromosome) throws IOException,
			InterruptedException {

		if (!oldChromosome.equals(chromosome)) {

			String chrFilename = legendPattern.replaceAll("\\$chr", chromosome);
			String myLegendFile = FileUtil.path(legendFile, chrFilename);

			if (!new File(myLegendFile).exists()) {
				throw new InterruptedException("ReferencePanel '"
						+ myLegendFile + "' not found.");
			}

			legendReader = new LegendFileReader(myLegendFile, population);
			legendReader.createIndex();
			legendReader.initSearch();

			oldChromosome = chromosome;

		}

		return legendReader;

	}

	private boolean isValid(String allele) {
		return allele.toUpperCase().equals("A")
				|| allele.toUpperCase().equals("C")
				|| allele.toUpperCase().equals("G")
				|| allele.toUpperCase().equals("T");
	}

	/*
	 * private SnpStats calculateStats(VariantContext snp) throws IOException,
	 * InterruptedException {
	 * 
	 * int ALLELE_A = 0; int ALLELE_B = 1;
	 * 
	 * char labels[] = new char[2]; int[] frequencies = new int[2];
	 * 
	 * // calculate maf frequencies[ALLELE_A] = snp.getHomRefCount();
	 * frequencies[ALLELE_B] = snp.getHomVarCount();
	 * 
	 * labels[ALLELE_A] = snp.getReference().getBaseString().charAt(0);
	 * labels[ALLELE_B] = snp.getAltAlleleWithHighestAlleleCount()
	 * .getBaseString().charAt(0);
	 * 
	 * int noAlleles = snp.getAlternateAlleles().size();
	 * 
	 * int total = frequencies[ALLELE_A] + frequencies[ALLELE_B];
	 * 
	 * // swap alleles: allele A = minor allele if (frequencies[ALLELE_A] >
	 * frequencies[ALLELE_B]) { char tempAlleleA = labels[ALLELE_A]; int
	 * tempFreqA = frequencies[ALLELE_A]; labels[ALLELE_A] = labels[ALLELE_B];
	 * frequencies[ALLELE_A] = frequencies[ALLELE_B]; labels[ALLELE_B] =
	 * tempAlleleA; frequencies[ALLELE_B] = tempFreqA; }
	 * 
	 * // load reference entry
	 * 
	 * String chromosome = snp.getChr(); int position = snp.getStart();
	 * LegendEntry refSnp = getReader(chromosome).findByPosition2(position);
	 * 
	 * SnpStats output = new SnpStats(); output.setChromosome(chromosome);
	 * output.setPosition(position); output.setAlleleA(labels[ALLELE_A]);
	 * output.setAlleleB(labels[ALLELE_B]);
	 * output.setFrequencyA(frequencies[ALLELE_A] / (float) total);
	 * output.setFrequencyB(frequencies[ALLELE_B] / (float) total);
	 * 
	 * // overlap if (refSnp != null) {
	 * 
	 * boolean sameAlleles = (labels[ALLELE_A] == refSnp.getAlleleB() &&
	 * labels[ALLELE_B] == refSnp .getAlleleA()) || (labels[ALLELE_B] ==
	 * refSnp.getAlleleB() && labels[ALLELE_A] == refSnp .getAlleleA());
	 * 
	 * if (refSnp.getType().equals("SNP")) {
	 * 
	 * // bring in the same order if (labels[ALLELE_A] == refSnp.getAlleleB() ||
	 * labels[ALLELE_B] == refSnp.getAlleleA()) { refSnp.swapAlleles(); }
	 * 
	 * output.setRefAlleleA(refSnp.getAlleleA());
	 * output.setRefAlleleB(refSnp.getAlleleB());
	 * output.setRefFrequencyA(refSnp.getFrequencyA());
	 * output.setRefFrequencyB(refSnp.getFrequencyB());
	 * 
	 * if (sameAlleles && noAlleles <= 2) {
	 * 
	 * // calculate chisq long[] observed = new long[2]; double[] expected = new
	 * double[2];
	 * 
	 * observed[0] = frequencies[ALLELE_A]; observed[1] = frequencies[ALLELE_B];
	 * 
	 * expected[0] = refSnp.getFrequencyA(); expected[1] =
	 * refSnp.getFrequencyB();
	 * 
	 * if (observed[0] == 0) { observed[0] = 1; } if (observed[1] == 0) {
	 * observed[1] = 1; }
	 * 
	 * if (expected[0] == 0) { expected[0] = 0.00001; } if (expected[1] == 0) {
	 * expected[1] = 0.00001; }
	 * 
	 * ChiSquareTest test = new ChiSquareTest(); double chisq =
	 * test.chiSquare(expected, observed); output.setChisq(chisq);
	 * output.setOverlapWithReference(true);
	 * 
	 * }
	 * 
	 * output.setType("SNP");
	 * 
	 * } else {
	 * 
	 * output.setChisq(Double.NaN); output.setOverlapWithReference(true);
	 * output.setType("INDEL");
	 * 
	 * } }
	 * 
	 * return output;
	 * 
	 * }
	 */
}
