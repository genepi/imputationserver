package genepi.imputationserver.steps.fastqc;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import genepi.imputationserver.steps.qc.SnpStats;
import genepi.imputationserver.steps.vcf.BGzipLineWriter;
import genepi.imputationserver.steps.vcf.VcfChunk;
import genepi.imputationserver.steps.vcf.VcfFile;
import genepi.imputationserver.steps.vcf.VcfFileUtil;
import genepi.imputationserver.util.GenomicTools;
import genepi.imputationserver.util.QualityControlObject;
import genepi.io.FileUtil;
import genepi.io.legend.LegendEntry;
import genepi.io.legend.LegendFileReader;
import genepi.io.text.GzipLineWriter;
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
import htsjdk.variant.vcf.VCFFileReader;
import htsjdk.variant.vcf.VCFHeader;

public class FastQCStatistics {

	public static final String X_PAR = "X.Pseudo.Auto";
	public static final String X_NON_PAR = "X.Non.Pseudo.Auto";

	private static double CALL_RATE = 0.5;
	private static int MIN_SNPS = 3;
	private static double OVERLAP = 0.5;

	String chunkFileDir = "tmp";
	String chunksDir = "tmp";
	String statDir = "tmp";
	String mafFile = "tmp/maf.txt";

	// input variables
	String population;
	int chunkSize;
	int phasingWindow;
	String input;
	String legendFile;
	int refSamples;

	// overall stats
	int overallChunks;
	int notFoundInLegend;
	int foundInLegend;
	int alleleMismatch;
	int alleleSwitch;
	int strandFlipSimple;
	int complicatedGenotypes;
	int strandFlipAndAlleleSwitch;
	int match;
	int lowCallRate;
	int filtered;
	int overallSnps;
	int monomorphic;
	int alternativeAlleles;
	int noSnps;
	int duplicates;
	int filterFlag;
	int invalidAlleles;
	int multiallelicSites;

	// chunk results
	int removedChunksSnps;
	int removedChunksOverlap;
	int removedChunksCallRate;

	public QualityControlObject run() throws IOException, InterruptedException {

		QualityControlObject qcObject = new QualityControlObject();

		qcObject.setMessage("");

		String[] vcfFilenames = FileUtil.getFiles(input, "*.vcf.gz$|*.vcf$");

		// MAF file for QC report
		LineWriter mafWriter = new LineWriter(mafFile);

		// excluded SNPS
		LineWriter excludedSnpsWriter = new LineWriter(FileUtil.path(statDir, "snps-excluded.txt"));
		excludedSnpsWriter.write("#Position" + "\t" + "FilterType" + "\t" + " Info");

		// excluded chunks
		LineWriter excludedChunkWriter = new LineWriter(FileUtil.path(statDir, "chunks-excluded.txt"));
		excludedChunkWriter.write(
				"#Chunk" + "\t" + "SNPs (#)" + "\t" + "Reference Overlap (%)" + "\t" + "Low Sample Call Rates (#)");

		// chrX infos
		LineWriter chrXInfoWriter = new LineWriter(FileUtil.path(statDir, "chrX-info.txt"));

		chrXInfoWriter.write("chrX log messages");

		// chrX haploid samples
		HashSet<String> hapSamples = new HashSet<String>();

		Arrays.sort(vcfFilenames);

		for (String vcfFilename : vcfFilenames) {

			VcfFile myvcfFile = VcfFileUtil.load(vcfFilename, chunkSize, true);		

			String chromosome = myvcfFile.getChromosome();
			boolean phased = myvcfFile.isPhased();
			
			if (VcfFileUtil.isChrX(chromosome)) {

				// split to par and non.par
				List<String> splits = prepareChrXEagle(vcfFilename, phased, chrXInfoWriter, hapSamples);

				for (String split : splits) {
					VcfFile _myvcfFile = VcfFileUtil.load(split, chunkSize, true);

					_myvcfFile.setChrX(true);

					// chrX
					processFile(_myvcfFile.getVcfFilename(), chromosome, chunkSize, phased, mafWriter, excludedSnpsWriter,
							excludedChunkWriter);
				}
			} else {
				// chr1-22
				processFile(vcfFilename, chromosome, chunkSize, phased, mafWriter, excludedSnpsWriter,
						excludedChunkWriter);

			}
		}

		mafWriter.close();

		excludedSnpsWriter.close();

		excludedChunkWriter.close();

		chrXInfoWriter.close();

		if (hapSamples.size() > 0) {
			LineWriter writer = new LineWriter(FileUtil.path(statDir, "chrX-samples.txt"));
			writer.write("The following samples have been changed from haploid to diploid");

			for (String sample : hapSamples) {
				writer.write(sample);
			}
			writer.close();

			qcObject.setMessage(
					"<b>Chromosome X Info:</b> For phasing/imputation we changed samples from haploid to diploid. Please check chrX-samples.txt");
		}

		qcObject.setSuccess(true);

		return qcObject;

	}

	public void processFile(String filename, String chromosome, int chunkSize, boolean phased, LineWriter mafWriter,
			LineWriter excludedSnpsWriter, LineWriter excludedChunkWriter) throws IOException, InterruptedException {

		Map<Integer, VcfChunk> chunks = new ConcurrentHashMap<Integer, VcfChunk>();

		FastVCFFileReader vcfReader = new FastVCFFileReader(filename);
		List<String> header = vcfReader.getFileHeader();

		String _contig = chromosome;

		// set X region as filename
		if (VcfFileUtil.isChrX(chromosome)) {
			_contig = filename.contains(X_NON_PAR) ? X_NON_PAR : X_PAR;
		}

		LineWriter metafileWriter = new LineWriter(FileUtil.path(chunkFileDir, _contig));
		LegendFileReader legendReader = getReader(chromosome);

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
				VcfChunk chunk = initChunk(_contig, chunkStart, chunkEnd, phased, header);
				chunks.put(chunkNumber, chunk);
			}

			int nextChunkNumber = chunkNumber + 1;
			int nextChunkStart = nextChunkNumber * chunkSize + 1;
			int extendedStart = nextChunkStart - phasingWindow;

			// is in the extended start of the next chunk?
			if (extendedStart >= 1 && snp.getStart() >= extendedStart) {
				if (chunks.get(nextChunkNumber) == null) {
					int nextChunkEnd = nextChunkStart + chunkSize - 1;
					VcfChunk nextChunk = initChunk(_contig, nextChunkStart, nextChunkEnd, phased,
							vcfReader.getFileHeader());
					chunks.put(nextChunkNumber, nextChunk);
				}
			}

			// load reference snp
			LegendEntry refSnp = legendReader.findByPosition2(snp.getStart());

			for (VcfChunk openChunk : chunks.values()) {
				if (snp.getStart() <= openChunk.getEnd() + phasingWindow) {
					processLine(snp, refSnp, openChunk.vcfChunkWriter, openChunk, mafWriter, excludedSnpsWriter);
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

	private void processLine(MinimalVariantContext snp, LegendEntry refSnp, BGzipLineWriter vcfWriter, VcfChunk chunk,
			LineWriter mafWriter, LineWriter excludedSnpsWriter) throws IOException, InterruptedException {

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
		if (snp.isMonomorphicInSamples()) {
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

			// allele-frequency check
			if (insideChunk) {
				if (!population.equals("mixed")) {
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
			int snpss = chunk.snpsPerSampleCount[i];
			double sampleCallRate = snpss / (double) chunk.overallSnpsChunk;

			if (sampleCallRate < CALL_RATE) {
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

			//create index
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

	public List<String> prepareChrXEagle(String filename, boolean phased, LineWriter chrXWriter,
			HashSet<String> hapSamples) throws IOException {

		List<String> paths = new Vector<String>();
		String nonPar = FileUtil.path(chunksDir, X_NON_PAR + ".vcf.gz");
		VariantContextWriter vcfChunkWriterNonPar = new VariantContextWriterBuilder().setOutputFile(nonPar)
				.setOption(Options.INDEX_ON_THE_FLY).setOption(Options.ALLOW_MISSING_FIELDS_IN_HEADER)
				.setOutputFileType(OutputType.BLOCK_COMPRESSED_VCF).build();

		String par = FileUtil.path(chunksDir, X_PAR + ".vcf.gz");
		VariantContextWriter vcfChunkWriterPar = new VariantContextWriterBuilder().setOutputFile(par)
				.setOption(Options.INDEX_ON_THE_FLY).setOption(Options.ALLOW_MISSING_FIELDS_IN_HEADER)
				.setOutputFileType(OutputType.BLOCK_COMPRESSED_VCF).build();

		VCFFileReader vcfReader = new VCFFileReader(new File(filename), true);

		VCFHeader header = vcfReader.getFileHeader();
		vcfChunkWriterNonPar.writeHeader(header);
		vcfChunkWriterPar.writeHeader(header);

		CloseableIterator<VariantContext> it = vcfReader.iterator();

		while (it.hasNext()) {

			VariantContext line = it.next();
			
			if(!line.getContig().equals("X")){
				line = new VariantContextBuilder(line).chr("X").make();
			}

			if (line.getStart() >= 2699521 && line.getStart() <= 154931043) {

				//not required anymore after updated version of Minimac4 (dbb99aeb27798e3e71a400421ffc1952bb1330f8)
				//line = makeDiploid(header.getGenotypeSamples(), line, phased, chrXWriter, hapSamples);
				vcfChunkWriterNonPar.add(line);

				if (!paths.contains(nonPar)) {
					paths.add(nonPar);
				}
			} else {

				vcfChunkWriterPar.add(line);

				if (!paths.contains(par)) {
					paths.add(par);
				}
			}

		}

		vcfReader.close();

		vcfChunkWriterPar.close();
		vcfChunkWriterNonPar.close();

		return paths;
	}

	public VariantContext makeDiploid(List<String> samples, VariantContext snp, boolean isPhased, LineWriter chrXWriter,
			HashSet<String> hapSamples) throws IOException {

		final GenotypesContext genotypes = GenotypesContext.create(samples.size());
		String ref = snp.getReference().getBaseString();

		for (final String name : samples) {

			Genotype genotype = snp.getGenotype(name);

			if (hapSamples.contains(name) && genotype.getPloidy() != 1) {

				chrXWriter.write("Converting haploid to diploid: " + name + " was already diploid at position "
						+ snp.getStart() + "\n");
			}

			if (genotype.getPloidy() == 1) {

				hapSamples.add(name);

				// better method available?
				boolean isRef = ref.equals(genotype.getGenotypeString(true)) ? true : false;
				final List<Allele> genotypeAlleles = new ArrayList<Allele>();
				Allele allele = Allele.create(genotype.getGenotypeString(), isRef);
				genotypeAlleles.add(allele);
				genotypeAlleles.add(allele);
				genotype = new GenotypeBuilder(name, genotypeAlleles).phased(isPhased).make();

			}

			genotypes.add(genotype);

		}

		return new VariantContextBuilder(snp).genotypes(genotypes).make();
	}

	private LegendFileReader getReader(String _chromosome) throws IOException, InterruptedException {
		
		// always use X for legend files chrX
		if(VcfFileUtil.isChrX(_chromosome)){
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

	public void setInput(String input) {
		this.input = input;
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

}
