package genepi.imputationserver.util;

import java.io.IOException;

import genepi.imputationserver.steps.fastqc.SnpStats;
import genepi.imputationserver.steps.fastqc.legend.LegendEntry;
import genepi.imputationserver.steps.vcf.MinimalVariantContext;

public class GenomicTools {

	private static final String GC = "GC";
	private static final String CG = "CG";
	private static final String TA = "TA";
	private static final String AT = "AT";
	private static final String CT = "CT";
	private static final String GA = "GA";
	private static final String TC = "TC";
	private static final String AG = "AG";
	private static final String GT = "GT";
	private static final String CA = "CA";
	private static final String TG = "TG";
	private static final String AC = "AC";
	private static final String T = "T";
	private static final String G = "G";
	private static final String C = "C";
	private static final String A = "A";

	public static boolean isValid(String allele) {
		return allele.toUpperCase().equals(A) || allele.toUpperCase().equals(C) || allele.toUpperCase().equals(G)
				|| allele.toUpperCase().equals(T);
	}

	public static boolean match(MinimalVariantContext snp, LegendEntry refEntry) {

		char studyRef = snp.getReferenceAllele().charAt(0);
		char studyAlt = snp.getAlternateAllele().charAt(0);
		char legendRef = refEntry.getAlleleA();
		char legendAlt = refEntry.getAlleleB();

		if (studyRef == legendRef && studyAlt == legendAlt) {

			return true;

		}

		return false;
	}

	public static boolean alleleSwitch(MinimalVariantContext snp, LegendEntry refEntry) {

		char studyRef = snp.getReferenceAllele().charAt(0);
		char studyAlt = snp.getAlternateAllele().charAt(0);

		char legendRef = refEntry.getAlleleA();
		char legendAlt = refEntry.getAlleleB();

		// all simple cases
		if (studyRef == legendAlt && studyAlt == legendRef) {

			return true;
		}

		return false;

	}

	public static boolean strandFlip(MinimalVariantContext snp, LegendEntry refEntry) {

		String studyGenotype = snp.getGenotype();
		String referenceGenotype = refEntry.getGenotype();

		if (studyGenotype.equals(AC)) {

			return referenceGenotype.equals(TG);

		} else if (studyGenotype.equals(CA)) {

			return referenceGenotype.equals(GT);

		} else if (studyGenotype.equals(AG)) {

			return referenceGenotype.equals(TC);

		} else if (studyGenotype.equals(GA)) {

			return referenceGenotype.equals(CT);

		} else if (studyGenotype.equals(TG)) {

			return referenceGenotype.equals(AC);

		} else if (studyGenotype.equals(GT)) {

			return referenceGenotype.equals(CA);

		} else if (studyGenotype.equals(CT)) {

			return referenceGenotype.equals(GA);

		} else if (studyGenotype.equals(TC)) {

			return referenceGenotype.equals(AG);

		}

		return false;

	}

	public static boolean complicatedGenotypes(MinimalVariantContext snp, LegendEntry refEntry) {

		String studyGenotype = snp.getGenotype();
		String referenceGenotype = refEntry.getGenotype();

		if ((studyGenotype.equals(AT) || studyGenotype.equals(TA))
				&& (referenceGenotype.equals(AT) || referenceGenotype.equals(TA))) {

			return true;

		} else if ((studyGenotype.equals(CG) || studyGenotype.equals(GC))
				&& (referenceGenotype.equals(CG) || referenceGenotype.equals(GC))) {

			return true;

		}
		return false;
	}

	public static boolean strandFlipAndAlleleSwitch(MinimalVariantContext snp, LegendEntry refEntry) {

		String studyGenotype = snp.getGenotype();
		String referenceGenotype = refEntry.getGenotype();

		if (studyGenotype.equals(AC)) {

			return referenceGenotype.equals(GT);

		} else if (studyGenotype.equals(CA)) {

			return referenceGenotype.equals(TG);

		} else if (studyGenotype.equals(AG)) {

			return referenceGenotype.equals(CT);

		} else if (studyGenotype.equals(GA)) {

			return referenceGenotype.equals(TC);

		} else if (studyGenotype.equals(TG)) {

			return referenceGenotype.equals(CA);

		} else if (studyGenotype.equals(GT)) {

			return referenceGenotype.equals(AC);

		} else if (studyGenotype.equals(CT)) {

			return referenceGenotype.equals(AG);

		} else if (studyGenotype.equals(TC)) {

			return referenceGenotype.equals(GA);

		}

		return false;

	}

	public static ChiSquareObject chiSquare(MinimalVariantContext snp, LegendEntry refSnp, boolean strandSwap,
			int size) {

		// calculate allele frequency

		double chisq = 0;

		int refN = size;

		double refA = refSnp.getFrequencyA();
		double refB = refSnp.getFrequencyB();

		int majorAlleleCount;
		int minorAlleleCount;

		if (!strandSwap) {
			majorAlleleCount = snp.getHomRefCount();
			minorAlleleCount = snp.getHomVarCount();

		} else {
			majorAlleleCount = snp.getHomVarCount();
			minorAlleleCount = snp.getHomRefCount();
		}

		int countRef = snp.getHetCount() + majorAlleleCount * 2;
		int countAlt = snp.getHetCount() + minorAlleleCount * 2;

		double p = countRef / (double) (countRef + countAlt);
		double q = countAlt / (double) (countRef + countAlt);
		double studyN = (snp.getNSamples() - snp.getNoCallCount()) * 2;

		double totalQ = q * studyN + refB * refN;
		double expectedQ = totalQ / (studyN + refN) * studyN;
		double deltaQ = q * studyN - expectedQ;

		chisq += (Math.pow(deltaQ, 2) / expectedQ) + (Math.pow(deltaQ, 2) / (totalQ - expectedQ));

		double totalP = p * studyN + refA * refN;
		double expectedP = totalP / (studyN + refN) * studyN;
		double deltaP = p * studyN - expectedP;

		chisq += (Math.pow(deltaP, 2) / expectedP) + (Math.pow(deltaP, 2) / (totalP - expectedP));

		return new ChiSquareObject(chisq, p, q);
	}

	public static boolean alleleMismatch(MinimalVariantContext snp, LegendEntry refEntry) {

		char studyRef = snp.getReferenceAllele().charAt(0);
		char studyAlt = snp.getAlternateAllele().charAt(0);
		char legendRef = refEntry.getAlleleA();
		char legendAlt = refEntry.getAlleleB();

		return studyRef != legendRef || studyAlt != legendAlt;

	}

	public static SnpStats calculateAlleleFreq(MinimalVariantContext snp, LegendEntry refSnp, int size)
			throws IOException, InterruptedException {

		boolean strandSwap = GenomicTools.strandFlipAndAlleleSwitch(snp, refSnp)
				|| GenomicTools.alleleSwitch(snp, refSnp);

		// calculate allele frequency
		SnpStats output = new SnpStats();

		int position = snp.getStart();

		ChiSquareObject chiObj = GenomicTools.chiSquare(snp, refSnp, strandSwap, size);

		char majorAllele;
		char minorAllele;

		if (!strandSwap) {
			majorAllele = snp.getReferenceAllele().charAt(0);
			minorAllele = snp.getAlternateAllele().charAt(0);
		} else {
			majorAllele = snp.getAlternateAllele().charAt(0);
			minorAllele = snp.getReferenceAllele().charAt(0);
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

}
