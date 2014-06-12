package genepi.minicloudmac.hadoop.preprocessing.vcf;

import genepi.hadoop.CacheStore;
import genepi.hadoop.HdfsUtil;
import genepi.hadoop.PreferenceStore;
import genepi.io.FileUtil;
import genepi.io.legend.LegendEntry;
import genepi.io.legend.LegendFileReader;
import genepi.io.text.LineReader;
import genepi.minicloudmac.hadoop.util.HdfsLineWriter;
import genepi.minicloudmac.hadoop.util.ParameterStore;
import genepi.minicloudmac.hadoop.validation.io.vcf.VcfChunk;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;

import org.apache.commons.math3.stat.inference.ChiSquareTest;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.broadinstitute.variant.variantcontext.Allele;
import org.broadinstitute.variant.variantcontext.VariantContext;
import org.broadinstitute.variant.variantcontext.VariantContext.Type;
import org.broadinstitute.variant.vcf.VCFCodec;
import org.broadinstitute.variant.vcf.VCFFileReader;
import org.broadinstitute.variant.vcf.VCFHeaderVersion;

public class MafMapper extends Mapper<LongWritable, Text, Text, Text> {

	private String folder;

	private LegendFileReader legendReader;

	private String oldChromosome = "";

	private String legendPattern;

	private String legendFile;

	private String population;

	private String output;

	private int monomorphic = 0;

	private int alternativeAlleles = 0;

	private int noSnps = 0;

	private int duplicates = 0;

	private int lastPos = 0;

	protected void setup(Context context) throws IOException,
			InterruptedException {

		// read parameters
		ParameterStore parameters = new ParameterStore(context);
		legendPattern = parameters.get(MafJob.LEGEND_PATTERN);
		population = parameters.get(MafJob.LEGEND_POPULATION);
		output = parameters.get(MafJob.OUTPUT_MAF);
		String hdfsPath = parameters.get(MafJob.LEGEND_HDFS);
		String legendFilename = FileUtil.getFilename(hdfsPath);

		// load files from cache
		CacheStore cache = new CacheStore(context.getConfiguration());
		legendFile = cache.getArchive(legendFilename);

		// create temp directory
		PreferenceStore store = new PreferenceStore(context.getConfiguration());
		folder = store.getString("minimac.tmp");
		folder = FileUtil.path(folder, context.getTaskAttemptID().toString());
		FileUtil.createDirectory(folder);

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
				output, context.getTaskAttemptID().toString()));

		String hdfsFilename = chunk.getVcfFilename() + "_" + chunk.getId();

		HdfsLineWriter newFileWriter = new HdfsLineWriter(hdfsFilename);
		StringBuilder chunkData = new StringBuilder();

		// +/- 1 Mbases
		int start = chunk.getStart() - 1000000;
		if (start < 1) {
			start = 1;
		}

		int end = chunk.getEnd() + 1000000;

		LineReader reader = new LineReader(vcfFilename);
		VCFFileReader vcfReader = new VCFFileReader(new File(vcfFilename));
		VCFCodec codec = new VCFCodec();

		codec.setVCFHeader(vcfReader.getFileHeader(), VCFHeaderVersion.VCF4_1);

		int foundInLegend = 0;
		int notFoundInLegend = 0;
		int alleleMismatch = 0;
		int toLessSamples = 0;
		int filtered = 0;
		int removedChunks = 0;
		int overallSnps = 0;
		int[] snpsPerSampleCount = null;

		chunkData.setLength(0);

		while (reader.next()) {

			String line = reader.get();

			if (line.startsWith("#")) {

				newFileWriter.write(line);

			} else {

				String tiles[] = line.split("\t", 6);
				int position = Integer.parseInt(tiles[1]);
				String ref = tiles[3];
				String alt = tiles[4];

				if (ref.toUpperCase().equals("A")
						|| ref.toUpperCase().equals("C")
						|| ref.toUpperCase().equals("G")
						|| ref.toUpperCase().equals("T")) {

					if (alt.toUpperCase().equals("A")
							|| alt.toUpperCase().equals("C")
							|| alt.toUpperCase().equals("G")
							|| alt.toUpperCase().equals("T")) {

						VariantContext snp = codec.decode(line);

						if (snpsPerSampleCount == null) {
							snpsPerSampleCount = new int[snp.getNSamples()];
						}

						boolean insideChunk = position >= chunk.getStart()
								&& position <= chunk.getEnd();

						// calc general statistics without filtering
						calcStats(snp, insideChunk);

						// start filtering
						if (!snp.isFiltered() && !snp.isIndel()
								&& !snp.isComplexIndel()
								&& snp.getHetCount() > 0) {

							LegendEntry refSnp = getReader(snp.getChr())
									.findByPosition2(snp.getStart());

							if (refSnp != null) {

								if (refSnp.getType().equals("SNP")) {

									foundInLegend++;

									char legendRef = refSnp.getAlleleA();
									char legendAlt = refSnp.getAlleleB();
									char studyRef = snp.getReference()
											.getBaseString().charAt(0);
									char studyAlt = snp
											.getAltAlleleWithHighestAlleleCount()
											.getBaseString().charAt(0);

									if (legendRef == studyRef
											&& studyAlt == legendAlt) {

										if (snp.getNoCallCount()
												/ (double) snp.getNSamples() <= 0.10) {

											// write only SNPs into minimac file
											// which came to this point
											if (position >= start
													&& position <= end) {
												overallSnps++;
												chunkData.append(line + "\n");

												// check if all samples have
												// enough SNPs
												int i = 0;
												for (String sample : snp
														.getSampleNamesOrderedByName()) {

													if (snp.getGenotype(sample)
															.isCalled()) {

														snpsPerSampleCount[i] += 1;
														
													}
													
													i++;
												}

											}

											// really?
											if (insideChunk) {

												if (!population.equals("mixed")) {

													SnpStats statistics = calculateAlleleFreq(
															snp, refSnp);
													statisticWriter
															.write(snp.getID()
																	+ "\t"
																	+ statistics
																			.toString());
												}
											}

										}

										else {

											if (insideChunk) {

												toLessSamples++;
												filtered++;

											}

										}

									}

									else {

										if (insideChunk) {

											alleleMismatch++;
											filtered++;

										}

									}

								}

							}
							

							else {

								chunkData.append(line + "\n");
								
								if (insideChunk) {

									notFoundInLegend++;

								}

							}

						}

						else {

							if (insideChunk) {

								filtered++;

							}

						}
					}
				}
			}
		}

		vcfReader.close();
		reader.close();
		legendReader.close();

		statisticWriter.write("");
		statisticWriter.close();

		// this checks if enough SNPs are included in each sample
		boolean acceptChunk = true;
		int i = 0;
		for (int it : snpsPerSampleCount) {
			if (it / (double) overallSnps < 0.9) {
				System.out.println("sample id "+ i);
				System.out.println("sample "+ (it / (double) overallSnps));
				System.out.println(it);
				System.out.println(overallSnps);
				acceptChunk = false;
				i++;
				break;
				
			}
			
		}
		
		// this checks if the amount of not found SNPs in the reference panel is smaller than 50 %. At least 3 SNPs must be included in each chunk
		if ((notFoundInLegend / (double) (foundInLegend +notFoundInLegend) < 0.5)  && overallSnps >= 3
				&& acceptChunk) {

			newFileWriter.write(chunkData.toString());
			newFileWriter.close();
			
			// update chunk
			chunk.setVcfFilename(hdfsFilename);
			context.write(new Text(chunk.getChromosome()),
					new Text(chunk.serialize()));
		}

		else {
			removedChunks++;
		}

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
		context.getCounter("minimac", "toLessSamples").increment(toLessSamples);
		context.getCounter("minimac", "filtered").increment(filtered);
		context.getCounter("minimac", "removedChunks").increment(removedChunks);
		// write updated value out

	}

	private void calcStats(VariantContext snp, boolean insideChunk) {

		if (snp.getHetCount() == 0 && !snp.isFiltered()) {

			if (insideChunk) {

				monomorphic++;

			}

		}

		int hetVarOnes = snp.getHetCount();

		int homVarOnes = snp.getHomVarCount() * 2;

		double af = (double) ((hetVarOnes + homVarOnes) / (double) (((snp
				.getNSamples() - snp.getNoCallCount()) * 2)));

		if (af > 0.5 && !snp.isFiltered()) {

			if (insideChunk) {

				alternativeAlleles++;

			}

		}

		if ((lastPos == snp.getStart() && lastPos > 0)) {

			if (insideChunk) {

				duplicates++;

			}

		}

		lastPos = snp.getStart();

		if (!snp.isSNP()) {

			if (insideChunk) {

				noSnps++;

			}

		}

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
