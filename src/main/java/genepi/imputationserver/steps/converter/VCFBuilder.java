package genepi.imputationserver.steps.converter;

import genepi.io.text.LineReader;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.reference.IndexedFastaSequenceFile;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.GenotypeBuilder;
import htsjdk.variant.variantcontext.GenotypesContext;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.VariantContextBuilder;
import htsjdk.variant.variantcontext.writer.Options;
import htsjdk.variant.variantcontext.writer.VariantContextWriter;
import htsjdk.variant.variantcontext.writer.VariantContextWriterBuilder;
import htsjdk.variant.vcf.VCFConstants;
import htsjdk.variant.vcf.VCFFormatHeaderLine;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderLine;
import htsjdk.variant.vcf.VCFHeaderLineType;
import htsjdk.variant.vcf.VCFHeaderVersion;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;

public class VCFBuilder {

	private static String GRCh37_PATH = "https://imputationserver.sph.umich.edu/static/downloads/human_g1k_v37.fasta";
	private String genome;
	private String reference;
	private String excludeList;
	private String outDirectory;
	private boolean split = true;
	private String tempDirectory = "temp";

	public VCFBuilder(String genome) {
		this.genome = genome;
	}

	public int build() throws MalformedURLException, IOException {

		VariantContextWriter vcfWriter = null;
		VCFHeader header = null;
		int counter = 0;
		String prev = "";
		boolean splitFiles = true;

		// create final output directory
		new File(outDirectory).mkdirs();

		// create temp directory
		new File(tempDirectory).mkdirs();

		// download reference
		if (reference.equals("v37")) {
			reference = "human_g1k_v37.fasta";
			if (!new File(reference).exists()) {
				System.out.println("Download Reference GRCh37: " + GRCh37_PATH + ".gz");
				FileUtils.copyURLToFile(new URL(GRCh37_PATH + ".gz"), new File(reference + ".gz"));
				FileUtils.copyURLToFile(new URL(GRCh37_PATH + ".fai"), new File(reference + ".fai"));
				FileUtil.gunzip(reference + ".gz", reference);
			}
		}

		// unzip if necessary
		if (genome.endsWith("zip")) {
			FileUtil.unzip(genome, tempDirectory);
			File[] files = new File(tempDirectory).listFiles();
			if (files.length == 1) {
				genome = files[0].getAbsolutePath();
			} else {
				throw new IOException("There is more than one file in your zip file.");
			}
		}
		LineReader genomeReader = new LineReader(genome);
		IndexedFastaSequenceFile refFasta = new IndexedFastaSequenceFile(new File(reference));

		// loop over 23andMe file
		while (genomeReader.next()) {

			// ignore header
			if (genomeReader.get().startsWith("#")) {
				continue;
			}

			// parse 23andMe line
			GenotypeLine line = new GenotypeLine();
			line.parse(genomeReader.get());
			String genotype = line.getGenotype();
			String chromosome = line.getChromosome();
			String g0 = genotype.substring(0, 1);
			String g1 = null;

			// Exclude indels, unsupported & multialleleic alleles
			if (line.getGenotype().contains("I") || genotype.contains("D") || genotype.contains("-")
					|| genotype.length() > 2) {
				continue;
			}

			// check exclude list
			if (excludeList != null && Arrays.asList(excludeList.split(",")).contains(line.getChromosome())) {
				continue;
			}

			// prepare header and files
			if (splitFiles && !chromosome.equals(prev)) {

				if (vcfWriter != null) {
					// System.out.println(prev + " - #sites: " + counter);
					vcfWriter.close();
				}

				String name = genome.substring(genome.lastIndexOf(File.separator) + 1, genome.lastIndexOf("."));
				header = generateHeader(name, chromosome);

				if (!split) {
					splitFiles = false;
				} else {
					name = name + "_chr" + chromosome;
				}

				VariantContextWriterBuilder builder = new VariantContextWriterBuilder()
						.setOutputFile(outDirectory + File.separator + name + ".vcf.gz")
						.unsetOption(Options.INDEX_ON_THE_FLY);

				vcfWriter = builder.build();

				vcfWriter.writeHeader(header);

				// prepare for next chromosome
				prev = chromosome;
				counter = 0;

			}

			// get position from fasta reference
			String refPos = refFasta.getSubsequenceAt(line.getChromosome(), line.getPos(), line.getPos())
					.getBaseString();

			Allele refAllele = Allele.create(refPos, true);
			Allele altAllele = null;
			boolean heterozygous = false;
			boolean homozygous = false;

			if (genotype.length() == 2) {
				g1 = genotype.substring(1, 2);
			}

			// heterozygous genotype check
			if (g1 != null && !g0.equals(g1)) {
				altAllele = Allele.create(!refPos.equals(g0) ? g0 : g1, false);
				heterozygous = true;
			}
			// homozygous genotype check
			else if (!refPos.equals(g0)) {
				altAllele = Allele.create(g0, false);
				homozygous = true;
			}
			// do nothing since its identical to the reference.
			else {
			}

			final List<Allele> alleles = new ArrayList<Allele>();
			alleles.add(refAllele);
			alleles.add(altAllele);

			if (heterozygous) {

				counter++;
				// set alleles for genotype
				vcfWriter.add(createVC(header, chromosome, line.getRsid(), alleles, alleles, line.getPos()));

			} else if (homozygous) {

				counter++;
				final List<Allele> genotypes = new ArrayList<Allele>();
				genotypes.add(altAllele);
				genotypes.add(altAllele);
				vcfWriter.add(createVC(header, chromosome, line.getRsid(), alleles, genotypes, line.getPos()));

			}

		} // while loop

		if (!splitFiles) {
			prev = "overall sites";
		}

		// System.out.println(prev + " - #sites: " + counter);
		System.out.println("\n All files written to: " + outDirectory);
		vcfWriter.close();
		refFasta.close();

		return 0;

	}

	private VCFHeader generateHeader(String name, String chromosome) {

		Set<VCFHeaderLine> headerLines = new HashSet<VCFHeaderLine>();
		Set<String> additionalColumns = new HashSet<String>();

		headerLines.add(new VCFHeaderLine(VCFHeaderVersion.VCF4_0.getFormatString(),
				VCFHeaderVersion.VCF4_0.getVersionString()));

		headerLines.add(new VCFFormatHeaderLine(VCFConstants.GENOTYPE_KEY, 1, VCFHeaderLineType.String, "Genotype"));

		additionalColumns.add(name);

		SAMSequenceDictionary sequenceDict = generateSequenceDictionary(chromosome);

		VCFHeader header = new VCFHeader(headerLines, additionalColumns);
		header.setSequenceDictionary(sequenceDict);

		return header;

	}

	private SAMSequenceDictionary generateSequenceDictionary(String chromosome) {

		SAMSequenceDictionary sequenceDict = new SAMSequenceDictionary();

		SAMSequenceRecord newSequence = new SAMSequenceRecord(chromosome, Chromosome.getChrLength(chromosome));

		sequenceDict.addSequence(newSequence);

		return sequenceDict;

	}

	private VariantContext createVC(VCFHeader header, String chrom, String rsid, List<Allele> alleles,
			List<Allele> genotype, int position) {

		final Map<String, Object> attributes = new HashMap<String, Object>();
		final GenotypesContext genotypes = GenotypesContext.create(header.getGenotypeSamples().size());

		for (final String name : header.getGenotypeSamples()) {
			final Genotype gt = new GenotypeBuilder(name, genotype).phased(false).make();
			genotypes.add(gt);
		}

		return new VariantContextBuilder("23andMe", chrom, position, position, alleles).genotypes(genotypes)
				.attributes(attributes).id(rsid).make();
	}

	public String getReference() {
		return reference;
	}

	public void setReference(String reference) {
		this.reference = reference;
	}

	public String getExcludeList() {
		return excludeList;
	}

	public void setExcludeList(String excludeList) {
		this.excludeList = excludeList;
	}

	public String getOutDirectory() {
		return outDirectory;
	}

	public void setOutDirectory(String outDirectory) {
		this.outDirectory = outDirectory;
	}

	public boolean isSplit() {
		return split;
	}

	public void setSplit(boolean split) {
		this.split = split;
	}

	public void setTempDirectory(String tempDirectory) {
		this.tempDirectory = tempDirectory;
	}

}
