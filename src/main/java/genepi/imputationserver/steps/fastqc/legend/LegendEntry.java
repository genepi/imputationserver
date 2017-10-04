package genepi.imputationserver.steps.fastqc.legend;

public class LegendEntry {

	private char alleleA;

	private char alleleB;

	private String rsId;

	private float frequencyA;

	private float frequencyB;

	private boolean frequencies = false;;

	private String type;

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

	public String getRsId() {
		return rsId;
	}

	public void setRsId(String rsId) {
		this.rsId = rsId;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public void swapAlleles() {
		char tempAlleleA = alleleA;
		float tempFreqA = frequencyA;
		alleleA = alleleB;
		frequencyA = frequencyB;
		alleleB = tempAlleleA;
		frequencyB = tempFreqA;
	}

	public boolean isAmbigous() {
		return (alleleA == 'A' && alleleA == 'T') || (alleleA == 'T' && alleleA == 'A')
				|| (alleleA == 'C' && alleleA == 'G') || (alleleA == 'G' && alleleA == 'C');
	}

	public void setFrequencies(boolean frequencies) {
		this.frequencies = frequencies;
	}

	public boolean hasFrequencies() {
		return frequencies;
	}

}
