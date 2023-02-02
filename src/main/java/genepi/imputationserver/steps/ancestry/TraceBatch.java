package genepi.imputationserver.steps.ancestry;

public class TraceBatch {

	private String batch;

	private int start;

	private int end;

	private String studyVcf;

	private int dim;

	private int dimHigh;

	private String outputStudyPc;

	private String outputStudyPopulation;

	public String getBatch() {
		return batch;
	}

	public void setBatch(String batch) {
		this.batch = batch;
	}

	public int getStart() {
		return start;
	}

	public void setStart(int start) {
		this.start = start;
	}

	public int getEnd() {
		return end;
	}

	public void setEnd(int end) {
		this.end = end;
	}

	public String getStudyVcf() {
		return studyVcf;
	}

	public void setStudyVcf(String studyVcf) {
		this.studyVcf = studyVcf;
	}

	public int getDim() {
		return dim;
	}

	public void setDim(int dim) {
		this.dim = dim;
	}

	public int getDimHigh() {
		return dimHigh;
	}

	public void setDimHigh(int dimHigh) {
		this.dimHigh = dimHigh;
	}

	public void setOutputStudyPc(String outputStudyPc) {
		this.outputStudyPc = outputStudyPc;
	}

	public String getOutputStudyPc() {
		return outputStudyPc;
	}

	public void setOutputStudyPopulation(String outputStudyPopulation) {
		this.outputStudyPopulation = outputStudyPopulation;
	}

	public String getOutputStudyPopulation() {
		return outputStudyPopulation;
	}

}
