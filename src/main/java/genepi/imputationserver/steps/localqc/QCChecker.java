package genepi.imputationserver.steps.localQC;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import genepi.imputationserver.steps.qc.SnpStats;
import genepi.imputationserver.steps.vcf.VcfChunk;
import genepi.imputationserver.steps.vcf.VcfFile;
import genepi.imputationserver.steps.vcf.VcfFileUtil;
import genepi.imputationserver.util.ChiSquareObject;
import genepi.imputationserver.util.GenomicTools;
import genepi.io.FileUtil;
import genepi.io.legend.LegendEntry;
import genepi.io.legend.LegendFileReader;
import genepi.io.text.LineWriter;
import htsjdk.samtools.util.CloseableIterator;
import htsjdk.tribble.index.IndexFactory;
import htsjdk.tribble.index.tabix.TabixFormat;
import htsjdk.tribble.index.tabix.TabixIndex;
import htsjdk.tribble.util.TabixUtils;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.writer.Options;
import htsjdk.variant.variantcontext.writer.VariantContextWriter;
import htsjdk.variant.variantcontext.writer.VariantContextWriterBuilder;
import htsjdk.variant.variantcontext.writer.VariantContextWriterBuilder.OutputType;
import htsjdk.variant.vcf.VCF3Codec;
import htsjdk.variant.vcf.VCFCodec;
import htsjdk.variant.vcf.VCFFileReader;
import htsjdk.variant.vcf.VCFHeader;

public class QCChecker {

	String population;
	int chunkSize;
	int phasingWindow;
	String input;
	String prefix = "";
	String legendFile;

	LegendFileReader legendReader;
	LineWriter logWriter;
	LineWriter chunkLogWriter;
	LineWriter chunkFileWriter;
	LineWriter statisticsWriter;

	private double CALL_RATE = 0.5;
	private int MIN_SNPS = 3;
	private double OVERLAP = 0.5;

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
	int monomorphic = 0;
	int alternativeAlleles = 0;
	int noSnps = 0;
	int duplicates = 0;
	int filterFlag = 0;
	int invalidAlleles = 0;

	int overallSnpsChunk = 0;
	int validSnpsChunk = 0;
	int foundInLegendChunk = 0;
	int notFoundInLegendChunk = 0;
	int[] snpsPerSampleCount = null;
	int removedChunksSnps = 0;
	int removedChunksOverlap = 0;
	int removedChunksCallRate = 0;

	public String getPopulation() {
		return population;
	}

	public void setPopulation(String population) {
		this.population = population;
	}

	public int getChunkSize() {
		return chunkSize;
	}

	public void setChunkSize(int chunkSize) {
		this.chunkSize = chunkSize;
	}

	public String getInput() {
		return input;
	}

	public void setInput(String input) {
		this.input = input;
	}

	public int getPhasingWindow() {
		return phasingWindow;
	}

	public void setPhasingWindow(int phasingWindow) {
		this.phasingWindow = phasingWindow;
	}

	public String getLegendFile() {
		return legendFile;
	}

	public void setLegendFile(String legendFile) {
		this.legendFile = legendFile;
	}

	void start() throws IOException, InterruptedException {

		long start = System.currentTimeMillis();

		String[] vcfFilenames = FileUtil.getFiles(input, "*.vcf.gz$|*.vcf$");

		logWriter = new LineWriter(FileUtil.path(prefix, "filtered.txt"));

		chunkLogWriter = new LineWriter(FileUtil.path(prefix, "chunks-log.txt"));

		chunkFileWriter = new LineWriter(FileUtil.path(prefix, "chunkfile.txt"));

		statisticsWriter = new LineWriter(FileUtil.path(prefix, "frequency-stats.txt"));

		for (String vcfFilename : vcfFilenames) {

			VcfFile myvcfFile = VcfFileUtil.load(vcfFilename, chunkSize, false);

			processFile(myvcfFile);

		}

		logWriter.close();

		chunkLogWriter.close();

		chunkFileWriter.close();

		statisticsWriter.close();

		printOverallStats();

		System.out.println("time " + (System.currentTimeMillis() - start) / 1000);

	}

	public void processFile(VcfFile myvcfFile) throws IOException, InterruptedException {

		TabixIndex idx = IndexFactory.createTabixIndex(new File(myvcfFile.getVcfFilename()), new VCFCodec(),
				TabixFormat.VCF, null);
		// idx.write(new File(myvcfFile.getVcfFilename()+".tbi"));

		VCFFileReader vcfReader = new VCFFileReader(new File(myvcfFile.getVcfFilename()),
				new File(myvcfFile.getVcfFilename() + TabixUtils.STANDARD_INDEX_EXTENSION), true);

		VCFHeader header = vcfReader.getFileHeader();

		for (int chunkNumber : myvcfFile.getChunks()) {

			int chunkStart = chunkNumber * chunkSize + 1;

			int chunkEnd = chunkStart + chunkSize - 1;

			int extendedStart = Math.max(chunkStart - phasingWindow, 1);

			int extendedEnd = chunkEnd + phasingWindow;

			String chunkName = "chunk_" + chunkNumber + "_" + chunkStart + "_" + chunkEnd + ".vcf.gz";

			VcfChunk chunk = new VcfChunk();
			chunk.setPos(chunkNumber);
			chunk.setChromosome(myvcfFile.getChromosome());
			chunk.setStart(chunkStart);
			chunk.setEnd(chunkEnd);
			chunk.setVcfFilename(chunkName);
			chunk.setPhased(myvcfFile.isPhased());

			// query with index
			CloseableIterator<VariantContext> snps = vcfReader.query(myvcfFile.getChromosome(),
					Math.max(extendedStart, 1), extendedEnd);

			VariantContextWriterBuilder builder = new VariantContextWriterBuilder().setOutputFile(chunkName)
					.setOption(Options.INDEX_ON_THE_FLY).setOption(Options.ALLOW_MISSING_FIELDS_IN_HEADER)
					.setOutputFileType(OutputType.BLOCK_COMPRESSED_VCF);

			VariantContextWriter vcfWriter = builder.build();

			vcfWriter.writeHeader(header);

			ArrayList<String> samples = vcfReader.getFileHeader().getSampleNamesInOrder();

			LegendFileReader legendReader = getReader(myvcfFile.getChromosome());

			while (snps.hasNext()) {

				processLine(snps.next(), vcfWriter, legendReader, chunk);

			}

			vcfWriter.close();

			chunkSummary(chunk, samples);

			resetChunk();
		}

		vcfReader.close();

	}

	private void processLine(VariantContext snp, VariantContextWriter vcfWriter, LegendFileReader legendReader,
			VcfChunk chunk) throws IOException, InterruptedException {

		int extendedStart = Math.max(chunk.getStart() - phasingWindow, 1);
		int extendedEnd = chunk.getEnd() + phasingWindow;

		int lastPos = 0;
		String ref = snp.getReference().getBaseString();
		String alt = snp.getAlternateAllele(0).getBaseString();
		int position = snp.getStart();

		boolean insideChunk = position >= chunk.getStart() && position <= chunk.getEnd();

		// filter invalid alleles
		if (!GenomicTools.isValid(ref) || !GenomicTools.isValid(alt)) {
			if (insideChunk) {
				logWriter.write("Invalid Alleles: " + snp.getID() + " (" + ref + "/" + alt + ")");
				invalidAlleles++;
				filtered++;
			}
			return;
		}

		// count duplicates
		if ((lastPos == snp.getStart() && lastPos > 0)) {

			if (insideChunk) {
				duplicates++;
				logWriter.write("FILTER - Duplicate: " + snp.getID() + " - pos: " + snp.getStart());
				// logWriter.write("COPY OF: " + tmp);
				filtered++;
			}

			lastPos = snp.getStart();
			return;

		}

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
					logWriter.write("FILTER - Duplicate " + snp.getID() + " - pos: " + snp.getStart());
					filtered++;
				} else {

					logWriter.write("FILTER - Flag is set: " + snp.getID() + " - pos: " + snp.getStart());
					filterFlag++;
					filtered++;
				}
			}
			return;
		}

		// alternative allele frequency
		int hetVarOnes = snp.getHetCount();
		int homVarOnes = snp.getHomVarCount() * 2;
		double af = (double) ((hetVarOnes + homVarOnes) / (double) (((snp.getNSamples() - snp.getNoCallCount()) * 2)));

		if (af > 0.5) {
			if (insideChunk) {
				alternativeAlleles++;
			}
		}

		// filter indels
		if (snp.isIndel() || snp.isComplexIndel()) {
			if (insideChunk) {
				logWriter.write("FILTER - InDel: " + snp.getID() + " - pos: " + snp.getStart());
				noSnps++;
				filtered++;
			}
			return;
		}

		// remove monomorphic snps
		// monomorphic only exclude 0/0;
		if (snp.isMonomorphicInSamples() || snp.getHetCount() == 2 * (snp.getNSamples() - snp.getNoCallCount())) {
			if (insideChunk) {
				// System.out.println(snp.getChr()+":"+snp.getStart());
				logWriter.write("FILTER - Monomorphic: " + snp.getID() + " - pos: " + snp.getStart());
				monomorphic++;
				filtered++;
			}
			return;
		}

		LegendEntry refSnp = getReader(snp.getContig()).findByPosition2(snp.getStart());
		// update Jul 8 2016: dont filter and add "allTypedSites"
		// minimac3 option
		if (refSnp == null) {

			if (insideChunk) {

				notFoundInLegend++;
				notFoundInLegendChunk++;
				vcfWriter.add(snp);
			}

		} else {

			if (insideChunk) {
				foundInLegend++;
				foundInLegendChunk++;
			}

			char legendRef = refSnp.getAlleleA();
			char legendAlt = refSnp.getAlleleB();
			char studyRef = snp.getReference().getBaseString().charAt(0);
			char studyAlt = snp.getAltAlleleWithHighestAlleleCount().getBaseString().charAt(0);

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

			/**
			 * simple allele switch check; ignore A/T C/G from above
			 **/
			else if (GenomicTools.alleleSwitch(snp, refSnp)) {

				if (insideChunk) {

					alleleSwitch++;
					logWriter.write("INFO - Allele switch: " + snp.getID() + " - pos: " + snp.getStart() + " (ref: "
							+ legendRef + "/" + legendAlt + ", data: " + studyRef + "/" + studyAlt + ")");
				}

			}

			/** simple strand swaps **/
			else if (GenomicTools.strandSwap(studyRef, studyAlt, legendRef, legendAlt)) {

				if (insideChunk) {

					strandSwitch1++;
					filtered++;
					logWriter.write("FILTER - Strand switch: " + snp.getID() + " - pos: " + snp.getStart() + " (ref: "
							+ legendRef + "/" + legendAlt + ", data: " + studyRef + "/" + studyAlt + ")");

				}
				return;

			}

			else if (GenomicTools.strandSwapAndAlleleSwitch(studyRef, studyAlt, legendRef, legendAlt)) {

				if (insideChunk) {

					filtered++;
					strandSwitch3++;
					logWriter.write("FILTER - Strand switch and Allele switch: " + snp.getID() + " - pos: "
							+ snp.getStart() + " (ref: " + legendRef + "/" + legendAlt + ", data: " + studyRef + "/"
							+ studyAlt + ")");

				}

				return;

			}

			// filter allele mismatches
			else if (GenomicTools.alleleMismatch(studyRef, studyAlt, legendRef, legendAlt)) {

				if (insideChunk) {
					logWriter.write("FILTER - Allele mismatch: " + snp.getID() + " - pos: " + snp.getStart() + " (ref: "
							+ legendRef + "/" + legendAlt + ", data: " + studyRef + "/" + studyAlt + ")");
					alleleMismatch++;
					filtered++;
				}
				return;
			}

			// filter low call rate
			if (snp.getNoCallCount() / (double) snp.getNSamples() > 0.10) {
				if (insideChunk) {
					logWriter.write("FILTER - Low call rate: " + snp.getID() + " - pos: " + snp.getStart() + " ("
							+ (1.0 - snp.getNoCallCount() / (double) snp.getNSamples()) + ")");
					lowCallRate++;
					filtered++;
				}
				return;
			}

			// allele-frequency check
			if (insideChunk) {
				if (!population.equals("mixed")) {
					SnpStats statistics;

					if (GenomicTools.strandSwapAndAlleleSwitch(studyRef, studyAlt, legendRef, legendAlt)
							|| GenomicTools.alleleSwitch(snp, refSnp)) {

						// swap alleles
						statistics = calculateAlleleFreq(snp, refSnp, true);
					}

					else {
						statistics = calculateAlleleFreq(snp, refSnp, false);
					}

					statisticsWriter.write(snp.getID() + "\t" + statistics.toString());
				}
				overallSnps++;
				overallSnpsChunk++;
			}

			// write SNPs
			if (position >= extendedStart && position <= extendedEnd) {

				vcfWriter.add(snp);
				validSnpsChunk++;

				// check if all samples have
				// enough SNPs

				if (insideChunk) {
					int i = 0;
					for (String sample : snp.getSampleNamesOrderedByName()) {
						if (snp.getGenotype(sample).isCalled()) {
							snpsPerSampleCount[i] += 1;
						}
						i++;
					}
				}
			}

		}
	}

	private void chunkSummary(VcfChunk chunk, ArrayList<String> samples) throws IOException {
		// this checks if enough SNPs are included in each sample
		boolean lowSampleCallRate = false;
		for (int i = 0; i < snpsPerSampleCount.length; i++) {
			int snpss = snpsPerSampleCount[i];
			double sampleCallRate = snpss / (double) overallSnpsChunk;

			if (sampleCallRate < CALL_RATE) {
				lowSampleCallRate = true;
				chunkLogWriter.write(chunk.getPos() + " Sample " + samples.get(i) + ": call rate: " + sampleCallRate);
			}

		}

		// this checks if the amount of not found SNPs in the reference
		// panel is
		// smaller than 50 %. At least 3 SNPs must be included in each chunk

		double overlap = foundInLegendChunk / (double) (foundInLegendChunk + notFoundInLegendChunk);

		if (overlap >= OVERLAP && foundInLegendChunk >= MIN_SNPS && !lowSampleCallRate && validSnpsChunk >= MIN_SNPS) {

			// update chunk
			chunk.setSnps(overallSnpsChunk);
			chunk.setInReference(foundInLegendChunk);
			chunkFileWriter.write(chunk.getChromosome() + "\t" + chunk.serialize());

		} else {

			chunkLogWriter.write(chunk.getPos() + " (Snps: " + overallSnpsChunk + ", Reference overlap: " + overlap
					+ ", low sample call rates: " + lowSampleCallRate + ")");

			if (overlap < OVERLAP) {
				removedChunksOverlap++;
			} else if (foundInLegendChunk < MIN_SNPS || validSnpsChunk < MIN_SNPS) {
				removedChunksSnps++;
			} else if (lowSampleCallRate) {
				removedChunksCallRate++;
			}

		}
	}

	private LegendFileReader getReader(String chromosome) throws IOException, InterruptedException {

		legendFile = legendFile.replaceAll("\\$chr", chromosome);
		String myLegendFile = FileUtil.path(legendFile);

		if (!new File(myLegendFile).exists()) {
			throw new InterruptedException("Legendfile '" + myLegendFile + "' not found.");
		}

		legendReader = new LegendFileReader(myLegendFile, population);
		legendReader.createIndex();
		legendReader.initSearch();

		return legendReader;

	}

	private SnpStats calculateAlleleFreq(VariantContext snp, LegendEntry refSnp, boolean strandSwap)
			throws IOException, InterruptedException {

		// calculate allele frequency
		SnpStats output = new SnpStats();

		int position = snp.getStart();

		ChiSquareObject chiObj = GenomicTools.chiSquare(snp, refSnp, strandSwap);

		char majorAllele;
		char minorAllele;

		if (!strandSwap) {
			majorAllele = snp.getReference().getBaseString().charAt(0);
			minorAllele = snp.getAltAlleleWithHighestAlleleCount().getBaseString().charAt(0);

		} else {
			majorAllele = snp.getAltAlleleWithHighestAlleleCount().getBaseString().charAt(0);
			minorAllele = snp.getReference().getBaseString().charAt(0);
		}

		output.setType("SNP");
		output.setPosition(position);
		output.setChromosome(snp.getContig());
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

	public static void main(String[] args) {

		QCChecker qcChecker = new QCChecker();
		qcChecker.setInput("/home/seb/Desktop/qc");
		qcChecker.setLegendFile("/home/seb/Desktop/qc/panel/hapmap_r22.chr$chr.CEU.hg19_impute.legend.gz");
		qcChecker.setChunkSize(20000000);
		qcChecker.setPhasingWindow(5000000);
		qcChecker.setPopulation("EUR");
		try {
			qcChecker.start();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private void printOverallStats() {
		System.out.println(" ");
		System.out.println("Statistics:");
		System.out.println("Alternative allele frequency > 0.5 site " + alternativeAlleles);

		System.out.println("Reference Overlap " + (foundInLegend / (foundInLegend + notFoundInLegend)) * 100);
		System.out.println("Match %" + match);
		System.out.println("Allele switch " + alleleSwitch);
		System.out.println("Strand flip " + strandSwitch1);
		System.out.println("Strand flip and allele switch: " + strandSwitch3);
		System.out.println("A/T, C/G genotypes: " + strandSwitch2);
		System.out.println(" ");
		System.out.println("Filtered sites:");
		System.out.println("Filter flag set:  " + filterFlag);
		System.out.println("Filtered sites:" + invalidAlleles);
		System.out.println("Duplicated sites: " + duplicates);
		System.out.println("NonSNP sites:" + noSnps);
		System.out.println("Monomorphic sites: " + monomorphic);
		System.out.println("Allele mismatch: " + alleleMismatch);
		System.out.println(" ");
		System.out.println(" ");
		System.out.println("Excluded sites in total: " + filtered);
		System.out.println("Remaining sites in total: " + overallSnps);
	}

	private void resetChunk() {

		overallSnpsChunk = 0;
		foundInLegendChunk = 0;
		notFoundInLegendChunk = 0;
		validSnpsChunk = 0;
		removedChunksSnps = 0;
		removedChunksOverlap = 0;
		removedChunksCallRate = 0;
		snpsPerSampleCount = null;
	}

	public String getPrefix() {
		return prefix;
	}

	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}

	public int getOverallSnps() {
		return overallSnps;
	}

	public void setOverallSnps(int overallSnps) {
		this.overallSnps = overallSnps;
	}

}
