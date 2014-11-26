package genepi.imputationserver.steps.qc;

import genepi.hadoop.CacheStore;
import genepi.hadoop.HdfsUtil;
import genepi.hadoop.ParameterStore;
import genepi.hadoop.PreferenceStore;
import genepi.hadoop.io.HdfsLineWriter;
import genepi.imputationserver.steps.vcf.VcfChunk;
import genepi.imputationserver.util.ChiSquareObject;
import genepi.imputationserver.util.GenomicTools;
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

	private static final double CALL_RATE = 0.5;

	private static final int MIN_SNPS = 3;

	private static final double OVERLAP = 0.5;

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

		int notFoundInLegend = 0;
		int foundInLegend = 0;
		int alleleMismatch = 0;
		int alleleSwitch = 0;
		int strandSwitch1 = 0;
		int strandSwitch2 = 0;
		int strandSwitch3 = 0;
		int match = 0;

		int lowCallRate = 0;
		int filtered = 0;
		int overallSnps = 0;
		int validSnps = 0;
		int monomorphic = 0;
		int alternativeAlleles = 0;
		int noSnps = 0;
		int duplicates = 0;
		int filterFlag = 0;
		int invalidAlleles = 0;

		int removedChunksSnps = 0;
		int removedChunksOverlap = 0;
		int removedChunksCallRate = 0;

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
				if (!GenomicTools.isValid(ref) || !GenomicTools.isValid(alt)) {
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
						logWriter.write("FILTER - Duplicate: " + snp.getID()
								+ " - pos: " + snp.getStart());
						//logWriter.write("COPY OF: " + tmp);
						filtered++;
					}

					lastPos = snp.getStart();
					continue;

				}

				String tmp = "FILTER - Duplicate: " + snp.getID() + " - pos: "
						+ snp.getStart();

				// update last pos only when not filtered
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
							logWriter.write("FILTER - Duplicate " + snp.getID()
									+ " - pos: " + snp.getStart());
							filtered++;
						} else {

							logWriter
									.write("FILTER - Flag is set: "
											+ snp.getID() + " - pos: "
											+ snp.getStart());
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
						logWriter.write("FILTER - InDel: " + snp.getID()
								+ " - pos: " + snp.getStart());
						noSnps++;
						filtered++;
					}
					continue;
				}

				// remove monomorphic snps
				// monomorphic only exclude 0/0;
				if (snp.isMonomorphicInSamples()
						|| snp.getHetCount() == 2 * (snp.getNSamples() - snp
								.getNoCallCount())) {
					if (insideChunk) {
						// System.out.println(snp.getChr()+":"+snp.getStart());
						logWriter.write("FILTER - Monomorphic: " + snp.getID()
								+ " - pos: " + snp.getStart());
						monomorphic++;
						filtered++;
					}
					continue;
				}

				LegendEntry refSnp = getReader(snp.getChr()).findByPosition2(
						snp.getStart());

				// not found in legend file, don't write to file (Talked to Chr)
				if (refSnp == null) {

					if (insideChunk) {

						overallSnps++;
						notFoundInLegend++;

						int i = 0;
						for (String sample : snp.getSampleNamesOrderedByName()) {
							if (snp.getGenotype(sample).isCalled()) {
								snpsPerSampleCount[i] += 1;
							}
							i++;
						}
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

					/** simple match of ref/alt in study and legend file **/
					if (GenomicTools.match(snp, refSnp)) {

						if (insideChunk) {
							match++;
						}

					}

					/** count A/T C/G genotypes **/
					else if (GenomicTools.complicatedGenotypes(snp, refSnp)) {

						if (insideChunk) {

							strandSwitch2++;

						}

					}

					/** simple allele switch check; ignore A/T C/G from above **/
					else if (GenomicTools.alleleSwitch(snp, refSnp)) {

						if (insideChunk) {
							
							alleleSwitch++;
							logWriter.write("INFO - Allele switch: "
									+ snp.getID() + " - pos: " + snp.getStart()
									+ " (ref: " + legendRef + "/" + legendAlt
									+ ", data: " + studyRef + "/" + studyAlt
									+ ")");
						}

					}

					/** simple strand swaps **/
					else if (GenomicTools.strandSwap(studyRef, studyAlt,
							legendRef, legendAlt)) {

						if (insideChunk) {
							
							strandSwitch1++;
							filtered++;
							logWriter.write("FILTER - Strand switch: "
									+ snp.getID() + " - pos: " + snp.getStart()
									+ " (ref: " + legendRef + "/" + legendAlt
									+ ", data: " + studyRef + "/" + studyAlt
									+ ")");
							
						}
						continue;

					}

					else if (GenomicTools.strandSwapAndAlleleSwitch(studyRef,
							studyAlt, legendRef, legendAlt)) {

						if (insideChunk) {

							filtered++;
							strandSwitch3++;
							logWriter
									.write("FILTER - Strand switch and Allele switch: "
											+ snp.getID()
											+ " - pos: "
											+ snp.getStart()
											+ " (ref: "
											+ legendRef
											+ "/"
											+ legendAlt
											+ ", data: "
											+ studyRef
											+ "/"
											+ studyAlt + ")");

						}

						continue;

					}

					// filter allele mismatches
					else if (GenomicTools.alleleMismatch(studyRef, studyAlt,
							legendRef, legendAlt)) {

						if (insideChunk) {
							logWriter.write("FILTER - Allele mismatch: "
									+ snp.getID() + " - pos: " + snp.getStart()
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
									.write("FILTER - Low call rate: "
											+ snp.getID()
											+ " - pos: "
											+ snp.getStart()
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
							SnpStats statistics;

							if (GenomicTools.strandSwapAndAlleleSwitch(
									studyRef, studyAlt, legendRef, legendAlt)
									|| GenomicTools.alleleSwitch(snp, refSnp)) {

								// swap alleles
								statistics = calculateAlleleFreq(snp, refSnp,
										true);
							}

							else {
								statistics = calculateAlleleFreq(snp, refSnp,
										false);
							}

							statisticWriter.write(snp.getID() + "\t"
									+ statistics.toString());
						}
						overallSnps++;
					}

					// write only SNPs into minimac file
					// which came up to this point
					if (position >= start && position <= end) {

						newFileWriter.write(line);
						validSnps++;

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
		boolean lowSampleCallRate = false;
		for (int i = 0; i < snpsPerSampleCount.length; i++) {
			int snps = snpsPerSampleCount[i];
			double sampleCallRate = snps / (double) overallSnps;
			
			if (sampleCallRate < CALL_RATE) {
				lowSampleCallRate = true;
				chunkWriter.write(chunk.toString()
						+ " Sample "
						+ vcfReader.getFileHeader().getSampleNamesInOrder()
								.get(i) + ": call rate: " + sampleCallRate);
			}

		}

		// this checks if the amount of not found SNPs in the reference panel is
		// smaller than 50 %. At least 3 SNPs must be included in each chunk

		double overlap = foundInLegend
				/ (double) (foundInLegend + notFoundInLegend);

		
		if (overlap >= OVERLAP && foundInLegend >= MIN_SNPS && !lowSampleCallRate && validSnps >= MIN_SNPS) {

			// update chunk
			chunk.setSnps(overallSnps);
			chunk.setInReference(foundInLegend);
			chunk.setVcfFilename(hdfsFilename);
			context.write(new Text(chunk.getChromosome()),
					new Text(chunk.serialize()));
		} else {

			chunkWriter.write(chunk.toString() + " (Snps: " + overallSnps
					+ ", Reference overlap: " + overlap
					+ ", low sample call rates: " + lowSampleCallRate + ")");

			if (overlap < OVERLAP) {
				removedChunksOverlap++;
			} else if (foundInLegend < MIN_SNPS || validSnps < MIN_SNPS) {
				removedChunksSnps++;
			} else if (lowSampleCallRate) {
				removedChunksCallRate++;
			}

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
		context.getCounter("minimac", "strandSwitch1").increment(strandSwitch1);
		context.getCounter("minimac", "strandSwitch2").increment(strandSwitch2);
		context.getCounter("minimac", "strandSwitch3").increment(strandSwitch3);
		context.getCounter("minimac", "match").increment(match);
		context.getCounter("minimac", "alleleSwitch").increment(alleleSwitch);

		context.getCounter("minimac", "toLessSamples").increment(lowCallRate);
		context.getCounter("minimac", "filtered").increment(filtered);
		context.getCounter("minimac", "removedChunksCallRate").increment(
				removedChunksCallRate);
		context.getCounter("minimac", "removedChunksOverlap").increment(
				removedChunksOverlap);
		context.getCounter("minimac", "removedChunksSnps").increment(
				removedChunksSnps);
		context.getCounter("minimac", "filterFlag").increment(filterFlag);
		context.getCounter("minimac", "invalidAlleles").increment(
				invalidAlleles);
		context.getCounter("minimac", "remainingSnps").increment(overallSnps);
		// write updated value out

	}

	private SnpStats calculateAlleleFreq(VariantContext snp,
			LegendEntry refSnp, boolean strandSwap) throws IOException,
			InterruptedException {

		// calculate allele frequency
		SnpStats output = new SnpStats();

		int position = snp.getStart();

		ChiSquareObject chiObj = GenomicTools
				.chiSquare(snp, refSnp, strandSwap);

		char majorAllele;
		char minorAllele;

		if (!strandSwap) {
			majorAllele = snp.getReference().getBaseString().charAt(0);
			minorAllele = snp.getAltAlleleWithHighestAlleleCount()
					.getBaseString().charAt(0);

		} else {
			majorAllele = snp.getAltAlleleWithHighestAlleleCount()
					.getBaseString().charAt(0);
			minorAllele = snp.getReference().getBaseString().charAt(0);
		}

		output.setType("SNP");
		output.setPosition(position);
		output.setChromosome(snp.getChr());
		output.setRefFrequencyA(refSnp.getFrequencyA());
		output.setRefFrequencyB(refSnp.getFrequencyB());
		output.setFrequencyA((float) chiObj.getP());
		output.setFrequencyB((float) chiObj.getQ());
		output.setChisq(chiObj.getChisq());
		output.setAlleleA(majorAllele);
		output.setAlleleB(minorAllele);
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

}
