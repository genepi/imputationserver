package genepi.imputationserver.steps.qc;

public class SnpStats {

	private String chromosome;

	private int position;

	private char alleleA = Byte.MAX_VALUE;

	private char alleleB = Byte.MAX_VALUE;

	private float frequencyA = Float.NaN;

	private float frequencyB = Float.NaN;

	private char refAlleleA = Byte.MAX_VALUE;

	private char refAlleleB = Byte.MAX_VALUE;

	private float refFrequencyA = Float.NaN;

	private float refFrequencyB = Float.NaN;
	
	private double chisq = Double.NaN;

	private boolean overlapWithReference = false;

	private String type;

	public String getChromosome() {
		return chromosome;
	}

	public void setChromosome(String chromosome) {
		this.chromosome = chromosome;
	}

	public int getPosition() {
		return position;
	}

	public void setPosition(int position) {
		this.position = position;
	}

	public char getAlleleA() {
		return alleleA;
	}

	public void setAlleleA(char alleleA) {
		this.alleleA = alleleA;
	}

	public char getAlleleB() {
		return alleleB;
	}

	public void setAlleleB(char alleleB) {
		this.alleleB = alleleB;
	}

	public float getFrequencyA() {
		return frequencyA;
	}

	public void setFrequencyA(float frequencyA) {
		this.frequencyA = frequencyA;
	}

	public float getFrequencyB() {
		return frequencyB;
	}

	public void setFrequencyB(float frequencyB) {
		this.frequencyB = frequencyB;
	}

	public char getRefAlleleA() {
		return refAlleleA;
	}

	public void setRefAlleleA(char refAlleleA) {
		this.refAlleleA = refAlleleA;
	}

	public char getRefAlleleB() {
		return refAlleleB;
	}

	public void setRefAlleleB(char refAlleleB) {
		this.refAlleleB = refAlleleB;
	}

	public float getRefFrequencyA() {
		return refFrequencyA;
	}

	public void setRefFrequencyA(float refFrequencyA) {
		this.refFrequencyA = refFrequencyA;
	}

	public float getRefFrequencyB() {
		return refFrequencyB;
	}

	public void setRefFrequencyB(float refFrequencyB) {
		this.refFrequencyB = refFrequencyB;
	}

	public double getChisq() {
		return chisq;
	}

	public void setChisq(double chisq) {
		this.chisq = chisq;
	}

	public boolean isOverlapWithReference() {
		return overlapWithReference;
	}

	public void setOverlapWithReference(boolean overlapWithReference) {
		this.overlapWithReference = overlapWithReference;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}
	
	@Override
	public String toString() {
		
		return chromosome + ":" + position + "\t" + alleleA + "\t" + alleleB
				+ "\t" + frequencyA + "\t" + frequencyB + "\t"
				+ (refAlleleA != Byte.MAX_VALUE ? refAlleleA : "NA") + "\t"
				+ (refAlleleB != Byte.MAX_VALUE ? refAlleleB : "NA") + "\t"
				+ (Float.isNaN(refFrequencyA) ? "NA":refFrequencyA) + "\t"
				+ (Float.isNaN(refFrequencyB) ? "NA" : refFrequencyB) + "\t"
				+ (Double.isNaN(chisq) ? "NA" : chisq) + "\t"
				+ overlapWithReference + "\t" + type;
	}

}
