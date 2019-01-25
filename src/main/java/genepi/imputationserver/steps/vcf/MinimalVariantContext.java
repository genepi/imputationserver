package genepi.imputationserver.steps.vcf;

public class MinimalVariantContext {

	public final static String NO_FILTERS = "";

	private int start;

	private String contig;

	private String referenceAllele;

	private String alternateAllele;

	private int hetCount;

	private int homRefCount;

	private int homVarCount;

	private int noCallCount;

	private int nSamples;

	private String rawLine;

	private String filters;

	private boolean[] genotypes;

	private String id = null;

	public MinimalVariantContext(int samples) {
		genotypes = new boolean[samples];
	}

	public int getHetCount() {
		return hetCount;
	}

	public void setHetCount(int hetCount) {
		this.hetCount = hetCount;
		this.id = null;
	}

	public int getHomRefCount() {
		return homRefCount;
	}

	public void setHomRefCount(int homRefCount) {
		this.homRefCount = homRefCount;
		this.id = null;
	}

	public int getHomVarCount() {
		return homVarCount;
	}

	public void setHomVarCount(int homVarCount) {
		this.homVarCount = homVarCount;
		this.id = null;
	}

	public int getNoCallCount() {
		return noCallCount;
	}

	public void setNoCallCount(int noCallCount) {
		this.noCallCount = noCallCount;
		this.id = null;
	}

	public int getStart() {
		return start;
	}

	public void setStart(int start) {
		this.start = start;
		this.id = null;
	}

	public String getContig() {
		return contig;
	}

	public void setContig(String contig) {
		this.contig = contig;
		this.id = null;
	}

	public String getReferenceAllele() {
		return referenceAllele;
	}

	public void setReferenceAllele(String referenceAllele) {
		this.referenceAllele = referenceAllele;
		this.id = null;
	}

	public String getAlternateAllele() {
		return alternateAllele;
	}

	public void setAlternateAllele(String alternateAllele) {
		this.alternateAllele = alternateAllele;
		this.id = null;
	}

	public void setNSamples(int nSamples) {
		this.nSamples = nSamples;
		this.id = null;
	}

	public int getNSamples() {
		return nSamples;
	}

	public void setRawLine(String rawLine) {
		this.rawLine = rawLine;
		this.id = null;
	}

	public String getRawLine() {
		return rawLine;
	}

	public boolean isFiltered() {
		return filters != null && !filters.isEmpty();
	}

	public String getFilters() {
		return filters;
	}

	public void setFilters(String filters) {
		this.filters = filters;
		this.id = null;
	}

	public boolean isIndel() {
		return getReferenceAllele().length() > 1 || getAlternateAllele().length() > 1;
	}

	public boolean isComplexIndel() {
		return getReferenceAllele().length() > 1 || getAlternateAllele().length() > 1;
	}

	public boolean isMonomorphicInSamples() {
		return (homRefCount + noCallCount == nSamples);
	}

	public void setCalled(int sample, boolean called) {
		genotypes[sample] = called;
		this.id = null;
	}

	public boolean isCalled(int sample) {
		return genotypes[sample];
	}

	public String toString() {
		if (id == null) {
			StringBuilder builder = new StringBuilder(7);
			builder.append(getContig());
			builder.append(":");
			builder.append(getStart());
			builder.append(":");
			builder.append(getReferenceAllele());
			builder.append(":");
			builder.append(getAlternateAllele());
			id = builder.toString();
		}
		return id;
	}

}
