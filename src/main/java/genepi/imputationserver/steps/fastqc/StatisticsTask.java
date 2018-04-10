package genepi.imputationserver.steps.fastqc;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import genepi.imputationserver.steps.fastqc.legend.LegendEntry;
import genepi.imputationserver.steps.fastqc.legend.LegendFileReader;
import genepi.imputationserver.steps.vcf.BGzipLineWriter;
import genepi.imputationserver.steps.vcf.FastVCFFileReader;
import genepi.imputationserver.steps.vcf.MinimalVariantContext;
import genepi.imputationserver.steps.vcf.VcfChunk;
import genepi.imputationserver.steps.vcf.VcfFile;
import genepi.imputationserver.steps.vcf.VcfFileUtil;
import genepi.imputationserver.util.GenomicTools;
import genepi.io.FileUtil;
import genepi.io.text.LineReader;
import genepi.io.text.LineWriter;
import htsjdk.samtools.util.CloseableIterator;
import htsjdk.tribble.util.TabixUtils;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.GenotypeBuilder;
import htsjdk.variant.variantcontext.GenotypesContext;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.VariantContextBuilder;
import htsjdk.variant.variantcontext.writer.Options;
import htsjdk.variant.variantcontext.writer.VariantContextWriter;
import htsjdk.variant.variantcontext.writer.VariantContextWriterBuilder;
import htsjdk.variant.variantcontext.writer.VariantContextWriterBuilder.OutputType;
import htsjdk.variant.vcf.VCFCodec;
import htsjdk.variant.vcf.VCFFileReader;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderVersion;

public class StatisticsTask implements ITask {

	public static final String X_PAR1 = "X.PAR1";
	public static final String X_PAR2 = "X.PAR2";
	public static final String X_NON_PAR = "X.nonPAR";

	private static double SAMPLE_CALL_RATE = 0.5;
	private static int MIN_SNPS = 3;
	private static double OVERLAP = 0.5;
	private static double CHR_X_MIXED_GENOTYPES = 0.1;

	private String chunkFileDir = "tmp";
	private String chunksDir = "tmp";
	private String statDir = "tmp";
	private String mafFile = "tmp/maf.txt";

	// input variables
	private String population;
	private int chunkSize;
	private int phasingWindow;
	private String[] vcfFilenames;
	private LineWriter excludedSnpsWriter;
	private String legendFile;
	private int refSamples;
	private boolean liftOver;
	private String build;

	// overall stats
	private int overallChunks;
	private int notFoundInLegend;
	private int foundInLegend;
	private int alleleMismatch;
	private int alleleSwitch;
	private int strandFlipSimple;
	private int complicatedGenotypes;
	private int strandFlipAndAlleleSwitch;
	private int match;
	private int lowCallRate;
	private int filtered;
	private int overallSnps;
	private int monomorphic;
	private int alternativeAlleles;
	private int noSnps;
	private int duplicates;
	private int filterFlag;
	private int invalidAlleles;
	private int multiallelicSites;

	private boolean chrXMissingRate = false;
	private boolean chrXPloidyError = false;

	// chunk results
	int removedChunksSnps;
	int removedChunksOverlap;
	int removedChunksCallRate;

	@Override
	public String getName() {
		return "Calculating QC Statistics";
	}

	public TaskResults run(ITaskProgressListener progressListener) throws IOException, InterruptedException {

		TaskResults qcObject = new TaskResults();

		qcObject.setMessage("");

		// MAF file for QC report
		LineWriter mafWriter = new LineWriter(mafFile);

		// excluded chunks
		String excludedChunkFile = FileUtil.path(statDir, "chunks-excluded.txt");
		LineWriter excludedChunkWriter = new LineWriter(excludedChunkFile);
		excludedChunkWriter.write(
				"#Chunk" + "\t" + "SNPs (#)" + "\t" + "Reference Overlap (%)" + "\t" + "Low Sample Call Rates (#)",
				false);

		String chrXInfoFile = FileUtil.path(statDir, "chrX-info.txt");
		LineWriter chrXInfoWriter = new LineWriter(chrXInfoFile);
		chrXInfoWriter.write("#Sample\tPosition", false);

		String typedOnleFile = FileUtil.path(statDir, "typed-only.txt");
		LineWriter typedOnlyWriter = new LineWriter(typedOnleFile);
		typedOnlyWriter.write("#Position", false);

		// chrX haploid samples
		HashSet<String> hapSamples = new HashSet<String>();

		int i = 0;
		for (String vcfFilename : vcfFilenames) {

			i++;
			if (progressListener != null) {
				progressListener.progress(getName() + " [" + i + "/" + vcfFilenames.length + "]\n\n" + "Analyze file "
						+ FileUtil.getFilename(vcfFilename) + "...");
			}

			VcfFile myvcfFile = VcfFileUtil.load(vcfFilename, chunkSize, true);

			String chromosome = myvcfFile.getChromosome();

			if (VcfFileUtil.isChrX(chromosome)) {

				// split to PAR1, PAR2 and nonPAR
				List<String> splits = prepareChrX(myvcfFile.getVcfFilename(), myvcfFile.isPhased(), chrXInfoWriter,
						hapSamples);

				for (String split : splits) {
					VcfFile _myvcfFile = VcfFileUtil.load(split, chunkSize, true);

					_myvcfFile.setChrX(true);

					// chrX
					processFile(_myvcfFile, mafWriter, excludedSnpsWriter, excludedChunkWriter, typedOnlyWriter);
				}
			} else {
				// chr1-22
				processFile(myvcfFile, mafWriter, excludedSnpsWriter, excludedChunkWriter, typedOnlyWriter);

			}
		}

		mafWriter.close();

		excludedChunkWriter.close();

		chrXInfoWriter.close();

		typedOnlyWriter.close();

		if (!excludedChunkWriter.hasData()) {
			FileUtil.deleteFile(excludedChunkFile);
		}

		if (!chrXInfoWriter.hasData()) {
			FileUtil.deleteFile(chrXInfoFile);
		}

		if (!typedOnlyWriter.hasData()) {
			FileUtil.deleteFile(typedOnleFile);
		}

		qcObject.setSuccess(true);

		return qcObject;

	}

	public void processFile(VcfFile myvcfFile, LineWriter mafWriter, LineWriter excludedSnpsWriter,
			LineWriter excludedChunkWriter, LineWriter typedOnlyWriter) throws IOException, InterruptedException {

		Map<Integer, VcfChunk> chunks = new ConcurrentHashMap<Integer, VcfChunk>();

		String filename = myvcfFile.getVcfFilename();

		FastVCFFileReader vcfReader = new FastVCFFileReader(filename);
		List<String> header = vcfReader.getFileHeader();

		String contig = myvcfFile.getChromosome();

		// set X region in filename
		if (VcfFileUtil.isChrX(myvcfFile.getChromosome())) {
			contig = X_NON_PAR;
			if (filename.contains(X_PAR1)) {
				contig = X_PAR1;
			} else if (filename.contains(X_PAR2)) {
				contig = X_PAR2;
			}
		}

		String metafile = FileUtil.path(chunkFileDir, contig);
		LineWriter metafileWriter = new LineWriter(metafile);
		LegendFileReader legendReader = getReader(myvcfFile.getChromosome());

		int samples = myvcfFile.getNoSamples();

		while (vcfReader.next()) {
			MinimalVariantContext snp = vcfReader.getVariantContext();
			int chunkNumber = snp.getStart() / chunkSize;
			if (snp.getStart() % chunkSize == 0) {
				chunkNumber = chunkNumber - 1;
			}

			// init current chunk only once
			if (chunks.get(chunkNumber) == null) {
				int chunkStart = chunkNumber * chunkSize + 1;
				int chunkEnd = chunkStart + chunkSize - 1;
				VcfChunk chunk = initChunk(contig, chunkStart, chunkEnd, myvcfFile.isPhased(), header);
				chunks.put(chunkNumber, chunk);
			}

			int nextChunkNumber = chunkNumber + 1;
			int nextChunkStart = nextChunkNumber * chunkSize + 1;
			int extendedStart = nextChunkStart - phasingWindow;

			// is in the extended start of the next chunk?
			if (extendedStart >= 1 && snp.getStart() >= extendedStart) {
				if (chunks.get(nextChunkNumber) == null) {
					int nextChunkEnd = nextChunkStart + chunkSize - 1;
					VcfChunk nextChunk = initChunk(contig, nextChunkStart, nextChunkEnd, myvcfFile.isPhased(),
							vcfReader.getFileHeader());
					chunks.put(nextChunkNumber, nextChunk);
				}
			}

			// load reference snp
			LegendEntry refSnp = legendReader.findByPosition(snp.getStart());

			for (VcfChunk openChunk : chunks.values()) {
				if (snp.getStart() <= openChunk.getEnd() + phasingWindow) {
					processLine(snp, refSnp, samples, openChunk.vcfChunkWriter, openChunk, mafWriter,
							excludedSnpsWriter, typedOnlyWriter);
				} else {
					// close open chunks
					openChunk.vcfChunkWriter.close();
					chunkSummary(openChunk, metafileWriter, excludedChunkWriter);
					chunks.values().remove(openChunk);
				}
			}

		}
		legendReader.close();
		vcfReader.close();

		// close all open chunks
		for (VcfChunk openChunk : chunks.values()) {
			openChunk.vcfChunkWriter.close();
			if (openChunk.lastPos >= openChunk.getStart()) {
				// System.out.println("Chunks " + open);
				chunkSummary(openChunk, metafileWriter, excludedChunkWriter);
			} else {
				new File(openChunk.getVcfFilename()).delete();
				new File(openChunk.getIndexFilename()).delete();
				overallChunks--;
			}
		}

		if (!metafileWriter.hasData()) {
			FileUtil.deleteFile(metafile);
		}

		metafileWriter.close();

	}

	private VcfChunk initChunk(String chr, int chunkStart, int chunkEnd, boolean phased, List<String> header)
			throws IOException {
		overallChunks++;

		String chunkName = null;

		chunkName = FileUtil.path(chunksDir, "chunk_" + chr + "_" + chunkStart + "_" + chunkEnd + ".vcf.gz");

		// init chunk
		VcfChunk chunk = new VcfChunk();
		chunk.setChromosome(chr);
		chunk.setStart(chunkStart);
		chunk.setEnd(chunkEnd);
		chunk.setVcfFilename(chunkName);
		chunk.setIndexFilename(chunkName + TabixUtils.STANDARD_INDEX_EXTENSION);
		chunk.setPhased(phased);

		BGzipLineWriter writer = new BGzipLineWriter(chunk.getVcfFilename());
		for (String headerLine : header) {
			writer.write(headerLine);
		}

		chunk.vcfChunkWriter = writer;

		return chunk;

	}

	private void processLine(MinimalVariantContext snp, LegendEntry refSnp, int samples, BGzipLineWriter vcfWriter,
			VcfChunk chunk, LineWriter mafWriter, LineWriter excludedSnpsWriter, LineWriter typedOnlyWriter)
			throws IOException, InterruptedException {

		int extendedStart = Math.max(chunk.getStart() - phasingWindow, 1);
		int extendedEnd = chunk.getEnd() + phasingWindow;

		String ref = snp.getReferenceAllele();
		int position = snp.getStart();

		String _contig = chunk.getChromosome();

		boolean insideChunk = position >= chunk.getStart() && position <= chunk.getEnd();

		if (snp.getAlternateAllele().contains(",")) {
			if (insideChunk) {
				excludedSnpsWriter.write(_contig + ":" + snp.getStart() + ":" + ref + ":" + snp.getAlternateAllele()
						+ "\t" + "Multiallelic Site");
				System.out.println(snp.getAlternateAllele());
				multiallelicSites++;
				filtered++;
			}
			return;
		}

		String alt = snp.getAlternateAllele() + "";

		String uniqueName = _contig + ":" + snp.getStart() + ":" + ref + ":" + alt;

		// filter invalid alleles
		if (!GenomicTools.isValid(ref) || !GenomicTools.isValid(alt)) {
			if (insideChunk) {
				excludedSnpsWriter.write(uniqueName + "\t" + "Invalid Alleles");
				invalidAlleles++;
				filtered++;
			}
			return;
		}

		// count duplicates

		if ((chunk.lastPos == snp.getStart() && chunk.lastPos > 0)) {

			if (insideChunk) {
				duplicates++;
				excludedSnpsWriter.write(uniqueName + "\t" + "Duplicate");
				filtered++;
			}

			chunk.lastPos = snp.getStart();
			return;

		}

		// update last pos only when not filtered
		if (!snp.isFiltered()) {
			chunk.lastPos = snp.getStart();
		}

		if (chunk.snpsPerSampleCount == null) {
			chunk.snpsPerSampleCount = new int[snp.getNSamples()];
			for (int i = 0; i < snp.getNSamples(); i++) {
				chunk.snpsPerSampleCount[i] = 0;
			}
		}

		// filter flag
		if (snp.isFiltered()) {
			if (insideChunk) {

				if (snp.getFilters().contains("DUP")) {
					duplicates++;
					excludedSnpsWriter.write(uniqueName + "\t" + "Filter Duplicate");
					filtered++;
				} else {

					excludedSnpsWriter.write(uniqueName + "\t" + "Filter Other");
					filterFlag++;
					filtered++;
				}
			}
			return;
		}

		// alternative allele frequency
		int hetVarOnes = snp.getHetCount();
		int homVarOnes = snp.getHomVarCount() * 2;
		double aaf = (double) ((hetVarOnes + homVarOnes) / (double) (((snp.getNSamples() - snp.getNoCallCount()) * 2)));

		if (aaf > 0.5) {
			if (insideChunk) {
				alternativeAlleles++;
			}
		}

		// filter indels
		if (snp.isIndel() || snp.isComplexIndel()) {
			if (insideChunk) {
				excludedSnpsWriter.write(uniqueName + "\t" + "InDel");
				noSnps++;
				filtered++;
			}
			return;
		}

		// monomorphic only excludes 0/0;
		if (samples > 1 && snp.isMonomorphicInSamples()) {
			if (insideChunk) {
				excludedSnpsWriter.write(uniqueName + "\t" + "Monomorphic");
				monomorphic++;
				filtered++;
			}
			return;
		}

		// update Jul 8 2016: dont filter and add "allTypedSites"
		// minimac3 option
		if (refSnp == null) {

			if (insideChunk) {

				notFoundInLegend++;
				chunk.notFoundInLegendChunk++;
				vcfWriter.write(snp.getRawLine());
				typedOnlyWriter.write(_contig + ":" + snp.getStart());
			}

		} else {

			if (insideChunk) {
				foundInLegend++;
				chunk.foundInLegendChunk++;
			}

			char legendRef = refSnp.getAlleleA();
			char legendAlt = refSnp.getAlleleB();
			char studyRef = snp.getReferenceAllele().charAt(0);
			char studyAlt = snp.getAlternateAllele().charAt(0);

			/** simple match of ref/alt in study and legend file **/
			if (GenomicTools.match(snp, refSnp)) {

				if (insideChunk) {
					match++;
				}

			}

			/** count A/T C/G genotypes **/
			else if (GenomicTools.complicatedGenotypes(snp, refSnp)) {

				if (insideChunk) {

					complicatedGenotypes++;

				}

			}

			/**
			 * simple allele switch check; ignore A/T C/G from above
			 **/
			else if (GenomicTools.alleleSwitch(snp, refSnp)) {

				if (insideChunk) {

					alleleSwitch++;
					/*
					 * logWriter.write("Allele switch" + snp.getID() + "\t" +
					 * chr + ":"+ snp.getStart() + "\t" + "ref: " + legendRef +
					 * "/" + legendAlt + "; data: " + studyRef + "/" + studyAlt
					 * + ")");
					 */
				}

			}

			/** simple strand swaps **/
			else if (GenomicTools.strandFlip(studyRef, studyAlt, legendRef, legendAlt)) {

				if (insideChunk) {

					strandFlipSimple++;
					filtered++;
					excludedSnpsWriter
							.write(uniqueName + "\t" + "Strand flip" + "\t" + "Ref:" + legendRef + "/" + legendAlt);

				}
				return;

			}

			else if (GenomicTools.strandFlipAndAlleleSwitch(studyRef, studyAlt, legendRef, legendAlt)) {

				if (insideChunk) {

					filtered++;
					strandFlipAndAlleleSwitch++;
					excludedSnpsWriter.write(uniqueName + "\t" + "Strand flip and Allele switch" + "\t" + "Ref:"
							+ legendRef + "/" + legendAlt);

				}

				return;

			}

			// filter allele mismatches
			else if (GenomicTools.alleleMismatch(studyRef, studyAlt, legendRef, legendAlt)) {

				if (insideChunk) {
					alleleMismatch++;
					filtered++;
					excludedSnpsWriter
							.write(uniqueName + "\t" + "Allele mismatch" + "\t" + "Ref:" + legendRef + "/" + legendAlt);
				}
				return;
			}

			// filter low call rate
			if (snp.getNoCallCount() / (double) snp.getNSamples() > 0.10) {
				if (insideChunk) {
					lowCallRate++;
					filtered++;
					excludedSnpsWriter.write(uniqueName + "\t" + "Low call rate" + "\t" + "Value: "
							+ (1.0 - snp.getNoCallCount() / (double) snp.getNSamples()));
				}
				return;
			}

			if (insideChunk) {

				// allele-frequency check
				if (!population.equals("mixed") && refSnp.hasFrequencies()) {
					SnpStats statistics;

					if (GenomicTools.strandFlipAndAlleleSwitch(studyRef, studyAlt, legendRef, legendAlt)
							|| GenomicTools.alleleSwitch(snp, refSnp)) {

						// swap alleles
						statistics = GenomicTools.calculateAlleleFreq(snp, refSnp, true, refSamples);
					}

					else {
						statistics = GenomicTools.calculateAlleleFreq(snp, refSnp, false, refSamples);
					}

					mafWriter.write(uniqueName + "\t" + statistics.toString());
				}
				overallSnps++;
				chunk.overallSnpsChunk++;
			}

			// write SNPs
			if (position >= extendedStart && position <= extendedEnd) {

				vcfWriter.write(snp.getRawLine());
				chunk.validSnpsChunk++;

				// check if all samples have
				// enough SNPs
				if (insideChunk) {

					for (int i = 0; i < snp.getNSamples(); i++) {
						if (snp.isCalled(i)) {
							chunk.snpsPerSampleCount[i] += 1;
						}
					}
				}
			}

		}
	}

	private void chunkSummary(VcfChunk chunk, LineWriter metafileWriter, LineWriter excludedChunkWriter)
			throws IOException {

		// this checks if enough SNPs are included in each sample
		boolean lowSampleCallRate = false;
		int countLowSamples = 0;
		for (int i = 0; i < chunk.snpsPerSampleCount.length; i++) {
			int snps = chunk.snpsPerSampleCount[i];
			double sampleCallRate = snps / (double) chunk.overallSnpsChunk;

			if (sampleCallRate < SAMPLE_CALL_RATE) {
				lowSampleCallRate = true;
				countLowSamples++;
			}

		}

		// this checks if the amount of not found SNPs in the reference
		// panel is
		// smaller than 50 %. At least 3 SNPs must be included in each chunk

		double overlap = chunk.foundInLegendChunk / (double) (chunk.foundInLegendChunk + chunk.notFoundInLegendChunk);

		if (overlap >= OVERLAP && chunk.foundInLegendChunk >= MIN_SNPS && !lowSampleCallRate
				&& chunk.validSnpsChunk >= MIN_SNPS) {

			// create index
			VcfFileUtil.createIndex(chunk.getVcfFilename());

			// update chunk
			chunk.setSnps(chunk.overallSnpsChunk);
			chunk.setInReference(chunk.foundInLegendChunk);
			metafileWriter.write(chunk.serialize());

		} else {

			excludedChunkWriter
					.write(chunk.toString() + "\t" + chunk.overallSnpsChunk + "\t" + overlap + "\t" + countLowSamples);

			if (overlap < OVERLAP) {
				removedChunksOverlap++;
			} else if (chunk.foundInLegendChunk < MIN_SNPS || chunk.validSnpsChunk < MIN_SNPS) {
				removedChunksSnps++;
			} else if (lowSampleCallRate) {
				removedChunksCallRate++;
			}

		}

	}

	public List<String> prepareChrX(String filename, boolean phased, LineWriter chrXInfoWriter,
			HashSet<String> hapSamples) throws IOException {

		List<String> paths = new Vector<String>();
		String nonPar = FileUtil.path(chunksDir, X_NON_PAR + ".vcf.gz");
		VariantContextWriter vcfChunkWriterNonPar = new VariantContextWriterBuilder().setOutputFile(nonPar)
				.setOption(Options.INDEX_ON_THE_FLY).setOption(Options.ALLOW_MISSING_FIELDS_IN_HEADER)
				.setOutputFileType(OutputType.BLOCK_COMPRESSED_VCF).build();

		String par1 = FileUtil.path(chunksDir, X_PAR1 + ".vcf.gz");
		VariantContextWriter vcfChunkWriterPar1 = new VariantContextWriterBuilder().setOutputFile(par1)
				.setOption(Options.INDEX_ON_THE_FLY).setOption(Options.ALLOW_MISSING_FIELDS_IN_HEADER)
				.setOutputFileType(OutputType.BLOCK_COMPRESSED_VCF).build();

		String par2 = FileUtil.path(chunksDir, X_PAR2 + ".vcf.gz");
		VariantContextWriter vcfChunkWriterPar2 = new VariantContextWriterBuilder().setOutputFile(par2)
				.setOption(Options.INDEX_ON_THE_FLY).setOption(Options.ALLOW_MISSING_FIELDS_IN_HEADER)
				.setOutputFileType(OutputType.BLOCK_COMPRESSED_VCF).build();

		VCFFileReader vcfReader = new VCFFileReader(new File(filename), true);

		VCFHeader header = vcfReader.getFileHeader();
		vcfChunkWriterNonPar.writeHeader(header);
		vcfChunkWriterPar1.writeHeader(header);
		vcfChunkWriterPar2.writeHeader(header);
		
		int mixedGenotypes[] = null;
		int count = 0;

		int nonParStart = 2699520;
		int nonParEnd = 154931044;

		if (build.equals("hg38")) {
			nonParStart = 2781479;
			nonParEnd = 155701383;
		}


		VCFCodec codec = new VCFCodec();
		codec.setVCFHeader(vcfReader.getFileHeader(), VCFHeaderVersion.VCF4_1);
		LineReader reader = new LineReader(filename);

		while (reader.next()) {

			String lineString = reader.get();

			if (!lineString.startsWith("#")) {

				String tiles[] = lineString.split("\t", 6);
				String ref = tiles[3];
				String alt = tiles[4];

				// filter invalid alleles
				if (!GenomicTools.isValid(ref) || !GenomicTools.isValid(alt)) {
					excludedSnpsWriter.write(tiles[0] + ":" + tiles[1] + ":" + ref + ":" + alt);
					invalidAlleles++;
					filtered++;
					continue;
				}

				//now decode, since it's valid
				VariantContext line = codec.decode(lineString);

				if (line.getContig().equals("23")) {
					line = new VariantContextBuilder(line).chr("X").make();
				}

				else if (line.getContig().equals("chr23")) {
					line = new VariantContextBuilder(line).chr("chrX").make();
				}

				if (line.getStart() < nonParStart) {

					vcfChunkWriterPar1.add(line);

					if (!paths.contains(par1)) {
						paths.add(par1);
					}

				}

				else if (line.getStart() >= nonParStart && line.getStart() <= nonParEnd) {

					count++;

					checkPloidy(header.getGenotypeSamples(), line, phased, chrXInfoWriter, hapSamples);

					mixedGenotypes = checkMixedGenotypes(mixedGenotypes, line);

					vcfChunkWriterNonPar.add(line);

					if (!paths.contains(nonPar)) {
						paths.add(nonPar);
					}

				}

				else {

					vcfChunkWriterPar2.add(line);

					if (!paths.contains(par2)) {
						paths.add(par2);
					}

				}

			}

		}

		if (mixedGenotypes != null) {
			for (int i = 0; i < mixedGenotypes.length; i++) {
				double missingRate = mixedGenotypes[i] / (double) count;
				if (missingRate > CHR_X_MIXED_GENOTYPES) {
					this.chrXMissingRate = true;
					break;
				}

			}
		}

		vcfReader.close();
		reader.close();

		vcfChunkWriterPar1.close();
		vcfChunkWriterPar2.close();
		vcfChunkWriterNonPar.close();

		return paths;
	}

	// mixed genotype: ./1; 1/.;
	private int[] checkMixedGenotypes(int[] mixedGenotypes, VariantContext line) {

		if (mixedGenotypes == null) {

			mixedGenotypes = new int[line.getNSamples()];
			for (int i = 0; i < line.getNSamples(); i++) {
				mixedGenotypes[i] = 0;
			}
		}

		for (int i = 0; i < line.getNSamples(); i++) {
			Genotype genotype = line.getGenotype(i);

			if (genotype.isMixed()) {
				mixedGenotypes[i] += 1;
			}

		}
		return mixedGenotypes;
	}

	public void checkPloidy(List<String> samples, VariantContext snp, boolean isPhased, LineWriter chrXInfoWriter,
			HashSet<String> hapSamples) throws IOException {

		for (final String name : samples) {

			Genotype genotype = snp.getGenotype(name);

			if (hapSamples.contains(name) && genotype.getPloidy() != 1) {
				chrXInfoWriter.write(name + "\t" + snp.getContig() + ":" + snp.getStart());
				this.chrXPloidyError = true;

			}

			if (genotype.getPloidy() == 1) {
				hapSamples.add(name);
			}

		}
	}

	private LegendFileReader getReader(String _chromosome) throws IOException, InterruptedException {

		// one file for all chrX legends
		if (VcfFileUtil.isChrX(_chromosome)) {
			_chromosome = "X";
		}

		String legendFile_ = legendFile.replaceAll("\\$chr", _chromosome);
		String myLegendFile = FileUtil.path(legendFile_);

		if (!new File(myLegendFile).exists()) {

			throw new InterruptedException("Legendfile '" + myLegendFile + "' not found.");

		}

		LegendFileReader legendReader = new LegendFileReader(myLegendFile, population);
		legendReader.createIndex();
		legendReader.initSearch();

		return legendReader;

	}

	public void setMafFile(String mafFile) {
		this.mafFile = mafFile;
	}

	public void setChunkFileDir(String chunkFileDir) {
		this.chunkFileDir = chunkFileDir;
	}

	public void setStatDir(String statDir) {
		this.statDir = statDir;
	}

	public void setChunksDir(String chunksDir) {
		this.chunksDir = chunksDir;
	}

	public void setPopulation(String population) {
		this.population = population;
	}

	public void setChunkSize(int chunkSize) {
		this.chunkSize = chunkSize;
	}

	public void setPhasingWindow(int phasingWindow) {
		this.phasingWindow = phasingWindow;
	}

	public void setLegendFile(String legendFile) {
		this.legendFile = legendFile;
	}

	public void setRefSamples(int refSamples) {
		this.refSamples = refSamples;
	}

	public void setLiftOver(boolean liftOver) {
		this.liftOver = liftOver;
	}

	public void setVcfFilenames(String[] vcfFilenames) {
		this.vcfFilenames = vcfFilenames;
	}

	public void setExcludedSnpsWriter(LineWriter excludedSnpsWriter) {
		this.excludedSnpsWriter = excludedSnpsWriter;
	}

	public int getOverallSnps() {
		return overallSnps;
	}

	public int getNotFoundInLegend() {
		return notFoundInLegend;
	}

	public int getFoundInLegend() {
		return foundInLegend;
	}

	public int getAlleleMismatch() {
		return alleleMismatch;
	}

	public int getAlleleSwitch() {
		return alleleSwitch;
	}

	public int getStrandFlipSimple() {
		return strandFlipSimple;
	}

	public int getComplicatedGenotypes() {
		return complicatedGenotypes;
	}

	public int getStrandFlipAndAlleleSwitch() {
		return strandFlipAndAlleleSwitch;
	}

	public int getMatch() {
		return match;
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

	public int getMonomorphic() {
		return monomorphic;
	}

	public int getAlternativeAlleles() {
		return alternativeAlleles;
	}

	public int getNoSnps() {
		return noSnps;
	}

	public int getDuplicates() {
		return duplicates;
	}

	public int getFilterFlag() {
		return filterFlag;
	}

	public int getInvalidAlleles() {
		return invalidAlleles;
	}

	public int getRemovedChunksSnps() {
		return removedChunksSnps;
	}

	public int getRemovedChunksOverlap() {
		return removedChunksOverlap;
	}

	public int getRemovedChunksCallRate() {
		return removedChunksCallRate;
	}

	public int getOverallChunks() {
		return overallChunks;
	}

	public int getMultiallelicSites() {
		return multiallelicSites;
	}

	public boolean isChrXMissingRate() {
		return chrXMissingRate;
	}

	public void setChrXMissingRate(boolean chrXMissingRate) {
		this.chrXMissingRate = chrXMissingRate;
	}

	public boolean isChrXPloidyError() {
		return chrXPloidyError;
	}

	public void setChrXPloidyError(boolean chrXPloidyError) {
		this.chrXPloidyError = chrXPloidyError;
	}

	public String getBuild() {
		return build;
	}

	public void setBuild(String build) {
		this.build = build;
	}

}
