package genepi.imputationserver.steps.qc;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;

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
import htsjdk.variant.vcf.VCFCodec;
import htsjdk.variant.vcf.VCFFileReader;
import htsjdk.variant.vcf.VCFHeader;

public class QCStatistics {

	String population;
	int chunkSize;
	int phasingWindow;
	String input;
	String legendFile;

	String outputMaf = "tmp/test.maf";
	String chunkfile = "tmp";
	String excludeLog = "tmp";
	String chunks;

	LegendFileReader legendReader;
	LineWriter logWriter;
	LineWriter chunkLogWriter;
	LineWriter chunkfileWriter;
	LineWriter mafWriter;

	private double CALL_RATE = 0.5;

	private int MIN_SNPS = 3;

	private double OVERLAP = 0.5;

	int amountChunks;
	
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

	//chunk specific
	int overallSnpsChunk = 0;
	int validSnpsChunk = 0;
	int foundInLegendChunk = 0;
	int notFoundInLegendChunk = 0;
	int[] snpsPerSampleCount = null;
	int removedChunksSnps = 0;
	int removedChunksOverlap = 0;
	int removedChunksCallRate = 0;

	boolean start() throws IOException, InterruptedException {

		String[] vcfFilenames = FileUtil.getFiles(input, "*.vcf.gz$|*.vcf$");
		
		// MAF file for QC report
		mafWriter = new LineWriter(outputMaf);
		
		// excluded SNPS
		logWriter = new LineWriter(FileUtil.path(excludeLog, "snps-excluded.txt"));
		
		// excluded chunks
		chunkLogWriter = new LineWriter(FileUtil.path(excludeLog, "chunks-excluded.txt"));

		for (String vcfFilename : vcfFilenames) {

			VcfFile myvcfFile = VcfFileUtil.load(vcfFilename, chunkSize, true);
			
			// chunkfile manifest
			chunkfileWriter = new LineWriter(FileUtil.path(chunkfile, myvcfFile.getChromosome()));

			processFile(myvcfFile);

			chunkfileWriter.close();

		}

		mafWriter.close();

		logWriter.close();

		chunkLogWriter.close();

		return true;

	}

	public void processFile(VcfFile myvcfFile) throws IOException, InterruptedException {

		VCFFileReader vcfReader = new VCFFileReader(new File(myvcfFile.getVcfFilename()), true);
		
		ArrayList<String> samples = vcfReader.getFileHeader().getSampleNamesInOrder();

		for (int chunkNumber : myvcfFile.getChunks()) {
			
			amountChunks++;

			int chunkStart = chunkNumber * chunkSize + 1;

			int chunkEnd = chunkStart + chunkSize - 1;

			int extendedStart = Math.max(chunkStart - phasingWindow, 1);

			int extendedEnd = chunkEnd + phasingWindow;

			String chunkName = FileUtil.path(chunks, "chunk_" + chunkNumber + "_" + chunkStart + "_" + chunkEnd + ".vcf.gz");

			VcfChunk chunk = new VcfChunk();
			chunk.setPos(chunkNumber);
			chunk.setChromosome(myvcfFile.getChromosome());
			chunk.setStart(chunkStart);
			chunk.setEnd(chunkEnd);
			chunk.setVcfFilename(chunkName);
			chunk.setIndexFilename(chunkName + TabixUtils.STANDARD_INDEX_EXTENSION);
			chunk.setPhased(myvcfFile.isPhased());

			// query with index
			CloseableIterator<VariantContext> snps = vcfReader.query(myvcfFile.getChromosome(),
					Math.max(extendedStart, 1), extendedEnd);

			VariantContextWriterBuilder builder = new VariantContextWriterBuilder().setOutputFile(chunkName)
					.setOption(Options.INDEX_ON_THE_FLY).setOption(Options.ALLOW_MISSING_FIELDS_IN_HEADER)
					.setOutputFileType(OutputType.BLOCK_COMPRESSED_VCF);

			VariantContextWriter vcfWriter = builder.build();

			vcfWriter.writeHeader(vcfReader.getFileHeader());

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
				logWriter.write("FILTER - Monomorphic: " + snp.getID() + " - pos: " + snp.getStart());
				monomorphic++;
				filtered++;
			}
			return;
		}

		LegendEntry refSnp = legendReader.findByPosition2(snp.getStart());
		
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
						statistics = GenomicTools.calculateAlleleFreq(snp, refSnp, true);
					}

					else {
						statistics = GenomicTools.calculateAlleleFreq(snp, refSnp, false);
					}

					mafWriter.write(snp.getID() + "\t" + statistics.toString());
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
				chunkLogWriter.write(chunk.toString() + " Sample " + samples.get(i) + ": call rate: " + sampleCallRate);
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
			chunkfileWriter.write(chunk.serialize());

		} else {

			chunkLogWriter.write(chunk.toString() + " (Snps: " + overallSnpsChunk + ", Reference overlap: " + overlap
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

	private void printOverallStats() {
		DecimalFormat df = new DecimalFormat("#.00");
		System.out.println(" ");
		System.out.println("Statistics:");
		System.out.println("Alternative allele frequency > 0.5 site " + alternativeAlleles);
		
		System.out.println("Reference Overlap " + df.format((foundInLegend / (double) (foundInLegend + notFoundInLegend)) * 100));
		System.out.println("Match " + match);
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
	
	public static void main(String[] args) {

		QCStatistics qcChecker = new QCStatistics();
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

	public int getOverallSnps() {
		return overallSnps;
	}

	public void setOverallSnps(int overallSnps) {
		this.overallSnps = overallSnps;
	}

	public String getOutputMaf() {
		return outputMaf;
	}

	public void setOutputMaf(String outputMaf) {
		this.outputMaf = outputMaf;
	}

	public String getChunkfile() {
		return chunkfile;
	}

	public void setChunkfile(String chunkfile) {
		this.chunkfile = chunkfile;
	}

	public String getExcludeLog() {
		return excludeLog;
	}

	public void setExcludeLog(String excludeLog) {
		this.excludeLog = excludeLog;
	}

	public String getChunks() {
		return chunks;
	}

	public void setChunks(String chunks) {
		this.chunks = chunks;
	}
	
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

	public LegendFileReader getLegendReader() {
		return legendReader;
	}

	public void setLegendReader(LegendFileReader legendReader) {
		this.legendReader = legendReader;
	}

	public LineWriter getLogWriter() {
		return logWriter;
	}

	public void setLogWriter(LineWriter logWriter) {
		this.logWriter = logWriter;
	}

	public LineWriter getChunkLogWriter() {
		return chunkLogWriter;
	}

	public void setChunkLogWriter(LineWriter chunkLogWriter) {
		this.chunkLogWriter = chunkLogWriter;
	}

	public LineWriter getChunkfileWriter() {
		return chunkfileWriter;
	}

	public void setChunkfileWriter(LineWriter chunkfileWriter) {
		this.chunkfileWriter = chunkfileWriter;
	}

	public LineWriter getMafWriter() {
		return mafWriter;
	}

	public void setMafWriter(LineWriter mafWriter) {
		this.mafWriter = mafWriter;
	}

	public double getCALL_RATE() {
		return CALL_RATE;
	}

	public void setCALL_RATE(double cALL_RATE) {
		CALL_RATE = cALL_RATE;
	}

	public int getNotFoundInLegend() {
		return notFoundInLegend;
	}

	public void setNotFoundInLegend(int notFoundInLegend) {
		this.notFoundInLegend = notFoundInLegend;
	}

	public int getFoundInLegend() {
		return foundInLegend;
	}

	public void setFoundInLegend(int foundInLegend) {
		this.foundInLegend = foundInLegend;
	}

	public int getAlleleMismatch() {
		return alleleMismatch;
	}

	public void setAlleleMismatch(int alleleMismatch) {
		this.alleleMismatch = alleleMismatch;
	}

	public int getAlleleSwitch() {
		return alleleSwitch;
	}

	public void setAlleleSwitch(int alleleSwitch) {
		this.alleleSwitch = alleleSwitch;
	}

	public int getStrandSwitch1() {
		return strandSwitch1;
	}

	public void setStrandSwitch1(int strandSwitch1) {
		this.strandSwitch1 = strandSwitch1;
	}

	public int getStrandSwitch2() {
		return strandSwitch2;
	}

	public void setStrandSwitch2(int strandSwitch2) {
		this.strandSwitch2 = strandSwitch2;
	}

	public int getStrandSwitch3() {
		return strandSwitch3;
	}

	public void setStrandSwitch3(int strandSwitch3) {
		this.strandSwitch3 = strandSwitch3;
	}

	public int getMatch() {
		return match;
	}

	public void setMatch(int match) {
		this.match = match;
	}

	public int getLowCallRate() {
		return lowCallRate;
	}

	public void setLowCallRate(int lowCallRate) {
		this.lowCallRate = lowCallRate;
	}

	public int getFiltered() {
		return filtered;
	}

	public void setFiltered(int filtered) {
		this.filtered = filtered;
	}

	public int getMonomorphic() {
		return monomorphic;
	}

	public void setMonomorphic(int monomorphic) {
		this.monomorphic = monomorphic;
	}

	public int getAlternativeAlleles() {
		return alternativeAlleles;
	}

	public void setAlternativeAlleles(int alternativeAlleles) {
		this.alternativeAlleles = alternativeAlleles;
	}

	public int getNoSnps() {
		return noSnps;
	}

	public void setNoSnps(int noSnps) {
		this.noSnps = noSnps;
	}

	public int getDuplicates() {
		return duplicates;
	}

	public void setDuplicates(int duplicates) {
		this.duplicates = duplicates;
	}

	public int getFilterFlag() {
		return filterFlag;
	}

	public void setFilterFlag(int filterFlag) {
		this.filterFlag = filterFlag;
	}

	public int getInvalidAlleles() {
		return invalidAlleles;
	}

	public void setInvalidAlleles(int invalidAlleles) {
		this.invalidAlleles = invalidAlleles;
	}

	public int getOverallSnpsChunk() {
		return overallSnpsChunk;
	}

	public void setOverallSnpsChunk(int overallSnpsChunk) {
		this.overallSnpsChunk = overallSnpsChunk;
	}

	public int getValidSnpsChunk() {
		return validSnpsChunk;
	}

	public void setValidSnpsChunk(int validSnpsChunk) {
		this.validSnpsChunk = validSnpsChunk;
	}

	public int getFoundInLegendChunk() {
		return foundInLegendChunk;
	}

	public void setFoundInLegendChunk(int foundInLegendChunk) {
		this.foundInLegendChunk = foundInLegendChunk;
	}

	public int getNotFoundInLegendChunk() {
		return notFoundInLegendChunk;
	}

	public void setNotFoundInLegendChunk(int notFoundInLegendChunk) {
		this.notFoundInLegendChunk = notFoundInLegendChunk;
	}

	public int[] getSnpsPerSampleCount() {
		return snpsPerSampleCount;
	}

	public void setSnpsPerSampleCount(int[] snpsPerSampleCount) {
		this.snpsPerSampleCount = snpsPerSampleCount;
	}

	public int getRemovedChunksSnps() {
		return removedChunksSnps;
	}

	public void setRemovedChunksSnps(int removedChunksSnps) {
		this.removedChunksSnps = removedChunksSnps;
	}

	public int getRemovedChunksOverlap() {
		return removedChunksOverlap;
	}

	public void setRemovedChunksOverlap(int removedChunksOverlap) {
		this.removedChunksOverlap = removedChunksOverlap;
	}

	public int getRemovedChunksCallRate() {
		return removedChunksCallRate;
	}

	public void setRemovedChunksCallRate(int removedChunksCallRate) {
		this.removedChunksCallRate = removedChunksCallRate;
	}

	public int getAmountChunks() {
		return amountChunks;
	}

	public void setAmountChunks(int amountChunks) {
		this.amountChunks = amountChunks;
	}

}
