package genepi.imputationserver.steps.fastqc;

import java.io.File;
import java.io.IOException;

import htsjdk.samtools.util.CloseableIterator;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;

public class Test {

	public static void main(String[] args) throws IOException {
		// String filename =
		// "/Users/lukas/Cloud/Genepi/Testdata/imputationserver/1000genomes.chrX.2.5M.small.recode.vcf.gz";

		// String filename =
		// "/Users/lukas/Downloads/G1K_P3_CHR_X_VCF_M3VCF_FILES/ALL.chrX.Non.Pseudo.Auto.phase3_v5.shapeit2_mvncall_integrated.noSingleton.genotypes.vcf.gz";

		String filename = "/media/lukas/Volume/test-data/data/panels/11.vcf.gz";

		long start = System.currentTimeMillis();
		loadUsingLib(filename);
		long end = System.currentTimeMillis();
		System.out.println("time: " + (end - start) + " ms");
		System.out.println("-------------------------");
		start = System.currentTimeMillis();
		loadFast(filename);
		end = System.currentTimeMillis();
		System.out.println("time: " + (end - start) + " ms");
	}

	public static int loadUsingLib(String vcfFilename) throws IOException {
		int noSnps = 0;
		int noSamples = 0;
		int noGenotypes = 0;

		int hetCount = 0;
		int homRefCount = 0;
		int homVarCount = 0;
		int noCalls = 0;

		VCFFileReader reader = new VCFFileReader(new File(vcfFilename), false);
		noSamples = reader.getFileHeader().getGenotypeSamples().size();
		CloseableIterator<VariantContext> iterator = reader.iterator();
		while (iterator.hasNext()) {
			VariantContext snp = iterator.next();

			hetCount += snp.getHetCount();
			homRefCount += snp.getHomRefCount();
			homVarCount += snp.getHomVarCount();
			noCalls += snp.getNoCallCount();
			noSnps++;
		}
		iterator.close();
		reader.close();
		System.out.println("Samples: " + noSamples);
		System.out.println("Snps: " + noSnps);
		System.out.println("Genotypes: " + noGenotypes);
		System.out.println("hetCount: " + hetCount);
		System.out.println("homRefCount: " + homRefCount);
		System.out.println("homVarCount: " + homVarCount);
		System.out.println("noCalls: " + noCalls);

		return 0;
	}

	public static int loadFast(String vcfFilename) throws IOException {

		int noSnps = 0;
		int noSamples = 0;
		int noGenotypes = 0;

		int hetCount = 0;
		int homRefCount = 0;
		int homVarCount = 0;
		int noCalls = 0;

		FastVCFFileReader reader = new FastVCFFileReader(vcfFilename);
		noSamples = reader.getSamplesCount();
		while (reader.next()) {
			MinimalVariantContext snp = reader.getVariantContext();
			hetCount += snp.getHetCount();
			homRefCount += snp.getHomRefCount();
			homVarCount += snp.getHomVarCount();
			noCalls += snp.getNoCallCount();
			noSnps++;

		}
		reader.close();

		System.out.println("Samples: " + noSamples);
		System.out.println("Snps: " + noSnps);
		System.out.println("Genotypes: " + noGenotypes);
		System.out.println("hetCount: " + hetCount);
		System.out.println("homRefCount: " + homRefCount);
		System.out.println("homVarCount: " + homVarCount);
		System.out.println("noCalls: " + noCalls);

		return 0;

	}
}
