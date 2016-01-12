package genepi.imputationserver.util;

import genepi.imputationserver.steps.qc.QualityControlMapper;
import genepi.io.legend.LegendEntry;

import org.broadinstitute.variant.variantcontext.VariantContext;

public class GenomicTools {

	public static boolean isValid(String allele) {
		return allele.toUpperCase().equals("A")
				|| allele.toUpperCase().equals("C")
				|| allele.toUpperCase().equals("G")
				|| allele.toUpperCase().equals("T");
	}

	public static boolean match(VariantContext snp, LegendEntry refEntry) {

		char studyRef = snp.getReference().getBaseString().charAt(0);
		char studyAlt = snp.getAltAlleleWithHighestAlleleCount()
				.getBaseString().charAt(0);
		char legendRef = refEntry.getAlleleA();
		char legendAlt = refEntry.getAlleleB();

		if (studyRef == legendRef && studyAlt == legendAlt) {

			return true;

		}

		return false;
	}

	/**
	 * it's only a match if chiSquare is under 300. This means there is no
	 * switch
	 **/
	public static boolean matchChiSquare(VariantContext snp,
			LegendEntry refEntry) {

		char studyRef = snp.getReference().getBaseString().charAt(0);
		char studyAlt = snp.getAltAlleleWithHighestAlleleCount()
				.getBaseString().charAt(0);
		char legendRef = refEntry.getAlleleA();
		char legendAlt = refEntry.getAlleleB();

		if (studyRef == legendRef && studyAlt == legendAlt) {

			return chiSquare(snp, refEntry, false).getChisq() <= 300;

		}

		return false;
	}

	public static boolean alleleSwitch(VariantContext snp, LegendEntry refEntry) {

		char studyRef = snp.getReference().getBaseString().charAt(0);
		char studyAlt = snp.getAltAlleleWithHighestAlleleCount()
				.getBaseString().charAt(0);

		char legendRef = refEntry.getAlleleA();
		char legendAlt = refEntry.getAlleleB();

		// all simple cases
		if (studyRef == legendAlt && studyAlt == legendRef) {

			return true;
		}

		return false;

	}

	public static boolean alleleSwitchChiSquare(VariantContext snp,
			LegendEntry refEntry) {

		char studyRef = snp.getReference().getBaseString().charAt(0);
		char studyAlt = snp.getAltAlleleWithHighestAlleleCount()
				.getBaseString().charAt(0);

		char legendRef = refEntry.getAlleleA();
		char legendAlt = refEntry.getAlleleB();

		String studyGenotype = new StringBuilder().append(studyRef)
				.append(studyAlt).toString();

		String referenceGenotype = new StringBuilder().append(legendRef)
				.append(legendAlt).toString();

		if ((studyGenotype.equals("AT") || studyGenotype.equals("TA"))
				&& (referenceGenotype.equals("AT") || referenceGenotype
						.equals("TA"))) {

			return chiSquare(snp, refEntry, false).getChisq() > 300;

		} else if ((studyGenotype.equals("CG") || studyGenotype.equals("GC"))
				&& (referenceGenotype.equals("CG") || referenceGenotype
						.equals("GC"))) {

			return chiSquare(snp, refEntry, false).getChisq() > 300;

		}

		// all other cases
		else if (studyRef == legendAlt && studyAlt == legendRef) {

			return true;
		}

		return false;

	}

	public static boolean strandSwap(char studyRef, char studyAlt,
			char legendRef, char legendAlt) {

		String studyGenotype = new StringBuilder().append(studyRef)
				.append(studyAlt).toString();

		String referenceGenotype = new StringBuilder().append(legendRef)
				.append(legendAlt).toString();

		if (studyGenotype.equals("AC")) {

			return referenceGenotype.equals("TG");

		} else if (studyGenotype.equals("CA")) {

			return referenceGenotype.equals("GT");

		} else if (studyGenotype.equals("AG")) {

			return referenceGenotype.equals("TC");

		} else if (studyGenotype.equals("GA")) {

			return referenceGenotype.equals("CT");

		} else if (studyGenotype.equals("TG")) {

			return referenceGenotype.equals("AC");

		} else if (studyGenotype.equals("GT")) {

			return referenceGenotype.equals("CA");

		} else if (studyGenotype.equals("CT")) {

			return referenceGenotype.equals("GA");

		} else if (studyGenotype.equals("TC")) {

			return referenceGenotype.equals("AG");

		}

		return false;

	}

	public static boolean complicatedGenotypes(VariantContext snp,
			LegendEntry refEntry) {

		char studyRef = snp.getReference().getBaseString().charAt(0);
		char studyAlt = snp.getAltAlleleWithHighestAlleleCount()
				.getBaseString().charAt(0);
		char legendRef = refEntry.getAlleleA();
		char legendAlt = refEntry.getAlleleB();

		String studyGenotype = new StringBuilder().append(studyRef)
				.append(studyAlt).toString();

		String referenceGenotype = new StringBuilder().append(legendRef)
				.append(legendAlt).toString();

		if ((studyGenotype.equals("AT") || studyGenotype.equals("TA"))
				&& (referenceGenotype.equals("AT") || referenceGenotype
						.equals("TA"))) {

			return true;

		} else if ((studyGenotype.equals("CG") || studyGenotype.equals("GC"))
				&& (referenceGenotype.equals("CG") || referenceGenotype
						.equals("GC"))) {

			return true;

		}
		return false;
	}

	public static boolean complicatedGenotypesChiSquare(VariantContext snp,
			LegendEntry refEntry) {

		char studyRef = snp.getReference().getBaseString().charAt(0);
		char studyAlt = snp.getAltAlleleWithHighestAlleleCount()
				.getBaseString().charAt(0);
		char legendRef = refEntry.getAlleleA();
		char legendAlt = refEntry.getAlleleB();

		String studyGenotype = new StringBuilder().append(studyRef)
				.append(studyAlt).toString();

		String referenceGenotype = new StringBuilder().append(legendRef)
				.append(legendAlt).toString();

		if ((studyGenotype.equals("AT") || studyGenotype.equals("TA"))
				&& (referenceGenotype.equals("AT") || referenceGenotype
						.equals("TA"))) {

			return chiSquare(snp, refEntry, false).getChisq() <= 300;

		} else if ((studyGenotype.equals("CG") || studyGenotype.equals("GC"))
				&& (referenceGenotype.equals("CG") || referenceGenotype
						.equals("GC"))) {

			return chiSquare(snp, refEntry, false).getChisq() <= 300;

		}
		return false;
	}

	public static boolean strandSwapAndAlleleSwitch(char studyRef,
			char studyAlt, char legendRef, char legendAlt) {

		String studyGenotype = new StringBuilder().append(studyRef)
				.append(studyAlt).toString();

		String referenceGenotype = new StringBuilder().append(legendRef)
				.append(legendAlt).toString();

		if (studyGenotype.equals("AC")) {

			return referenceGenotype.equals("GT");

		} else if (studyGenotype.equals("CA")) {

			return referenceGenotype.equals("TG");

		} else if (studyGenotype.equals("AG")) {

			return referenceGenotype.equals("CT");

		} else if (studyGenotype.equals("GA")) {

			return referenceGenotype.equals("TC");

		} else if (studyGenotype.equals("TG")) {

			return referenceGenotype.equals("CA");

		} else if (studyGenotype.equals("GT")) {

			return referenceGenotype.equals("AC");

		} else if (studyGenotype.equals("CT")) {

			return referenceGenotype.equals("AG");

		} else if (studyGenotype.equals("TC")) {

			return referenceGenotype.equals("GA");

		}

		return false;

	}

	public static ChiSquareObject chiSquare(VariantContext snp,
			LegendEntry refSnp, boolean strandSwap) {

		// calculate allele frequency

		double chisq = 0;

		int refN = getPanelSize(QualityControlMapper.PANEL_ID);
		
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

		chisq += (Math.pow(deltaQ, 2) / expectedQ)
				+ (Math.pow(deltaQ, 2) / (totalQ - expectedQ));

		double totalP = p * studyN + refA * refN;
		double expectedP = totalP / (studyN + refN) * studyN;
		double deltaP = p * studyN - expectedP;

		chisq += (Math.pow(deltaP, 2) / expectedP)
				+ (Math.pow(deltaP, 2) / (totalP - expectedP));

		return new ChiSquareObject(chisq, p, q);
	}

	public static int getPanelSize(String panelId) {
		switch (panelId) {
		case "phase1":
			return 1092;
		case "phase3":
			return 2535;
		case "hrc":
			return 32611;
		case "hapmap2":
			return 1301;
		case "caapa":
			return 883;
		default:
			return 1092;
		}
	}

	public static int getPopSize(String pop) {

		switch (pop) {
		case "eur":
			return 11418;
		case "afr":
			return 17469;
		case "asn":
		case "sas":
		case "eas":
			return 14269;
		default:
			return 15000;
		}
	}

	public static boolean alleleMismatch(char studyRef, char studyAlt,
			char referenceRef, char referenceAlt) {

		return studyRef != referenceRef || studyAlt != referenceAlt;

	}

}
