package genepi.imputationserver.steps.fastqc;

import java.io.File;
import java.io.IOException;

import htsjdk.samtools.util.CloseableIterator;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;

public class Test2 {

	public static void main(String[] args) throws IOException {
		// String filename =
		// "/Users/lukas/Cloud/Genepi/Testdata/imputationserver/1000genomes.chrX.2.5M.small.recode.vcf.gz";

		// String filename =
		// "/Users/lukas/Downloads/G1K_P3_CHR_X_VCF_M3VCF_FILES/ALL.chrX.Non.Pseudo.Auto.phase3_v5.shapeit2_mvncall_integrated.noSingleton.genotypes.vcf.gz";

		/*String filename = "test-data/data/single/minimac_test.50.vcf.gz";
		compare(filename);*/
		
		String filename = "test-data/data/simulated-chip-3chr-qc/1000genomes.chr1.HumanHap550.small.recode.vcf.gz";
		System.out.println(filename+":");
		compare(filename);
	}

	public static int compare(String vcfFilename) throws IOException {

		FastVCFFileReader reader2 = new FastVCFFileReader(vcfFilename);

		VCFFileReader reader = new VCFFileReader(new File(vcfFilename), false);
		CloseableIterator<VariantContext> iterator = reader.iterator();
		while (iterator.hasNext()) {
			VariantContext snp = iterator.next();

			reader2.next();
			MinimalVariantContext snp2 = reader2.getVariantContext();

			if (snp.isMonomorphicInSamples() != snp2.isMonomorphicInSamples()) {
				System.out.println("isMonomorphicInSamples: " + snp2.isMonomorphicInSamples() + " --> " + snp2.getRawLine());
			}
			if (snp.getHomVarCount() != snp2.getHomVarCount()) {
				System.out.println("getHomVarCount: " + snp.getHomVarCount() + " vs. " + snp2.getHomVarCount());
			}
			if (snp.getHetCount() != snp2.getHetCount()) {
				System.out.println("getHetCount: " + snp.getHetCount() + " vs. " + snp2.getHetCount());
			}
			if (snp.getNSamples() != snp2.getNSamples()) {
				System.out.println("getNSamples: " + snp.getNSamples() + " vs. " + snp2.getNSamples());
			}
			if (snp.getNoCallCount() != snp2.getNoCallCount()) {
				System.out.println("getNoCallCount: " + snp.getNoCallCount() + " vs. " + snp2.getNoCallCount());
			}
			if (snp.isMonomorphicInSamples() != snp2.isMonomorphicInSamples()) {
				System.out.println("isMonomorphicInSamples: " + snp.isMonomorphicInSamples() + " vs. "
						+ snp2.isMonomorphicInSamples());
			}
			if (snp.isIndel() != snp2.isIndel()) {
				System.out.println("isIndel: " + snp.isIndel() + " vs. " + snp2.isIndel());
			}
			if (snp.isComplexIndel() != snp2.isComplexIndel()) {
				System.out.println("isIndel: " + snp.isComplexIndel() + " vs. " + snp2.isComplexIndel());
			}
			if (snp.isFiltered() != snp2.isFiltered()) {
				System.out.println("isFiltered: " + snp.isFiltered() + " vs. " + snp2.isFiltered() + " line: "
						+ snp2.getRawLine());
			}
			if (snp.getAlternateAlleles().size() != snp2.getAlternateAllele().length()) {
				System.out.println("allt size: " + snp.getAlternateAlleles().size() + " vs. "
						+ snp2.getAlternateAllele().length() + " line: " + snp2.getRawLine());
			}
			if (!snp.getAlternateAlleles().get(0).getBaseString().equals(snp2.getAlternateAllele().subSequence(0,1))) {
				System.out.println("allt size: " + snp.getAlternateAlleles() + " vs. "
						+ snp2.getAlternateAllele().subSequence(0,1) + " line: " + snp2.getRawLine());
			}
		}
		reader2.close();
		reader.close();
		return 0;

	}

}
