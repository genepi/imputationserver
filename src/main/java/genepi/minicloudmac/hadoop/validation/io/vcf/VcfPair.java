package genepi.minicloudmac.hadoop.validation.io.vcf;

import genepi.minicloudmac.hadoop.validation.io.AbstractPair;

import java.util.Set;

public class VcfPair extends AbstractPair {

	private Set<Integer> chunks;

	private Set<String> chromosomes;

	private String chromosome;

	private String vcfFilename;

	private String indexFilename;

	private int noSamples = -1;

	private int noSnps = -1;

	private boolean phased = true;

	public VcfPair() {

	}

	@Override
	public int getNoSnps() {
		return noSnps;
	}

	public void setNoSnps(int noSnps) {
		this.noSnps = noSnps;
	}

	public Set<String> getChromosomes() {
		return chromosomes;
	}

	public String getChromosome() {
		return chromosomes.iterator().next();
	}

	public String getVcfFilename() {
		return vcfFilename;
	}

	public String getIndexFilename() {
		return indexFilename;
	}

	public int getNoSamples() {
		return noSamples;
	}

	public Set<Integer> getChunks() {
		return chunks;
	}

	public void setChunks(Set<Integer> chunks) {
		this.chunks = chunks;
	}

	public void setChromosome(String chromosome) {
		this.chromosome = chromosome;
	}

	public void setVcfFilename(String vcfFilename) {
		this.vcfFilename = vcfFilename;
	}

	public void setIndexFilename(String indexFilename) {
		this.indexFilename = indexFilename;
	}

	public void setNoSamples(int noSamples) {
		this.noSamples = noSamples;
	}

	public void setChromosomes(Set<String> chromosomes) {
		this.chromosomes = chromosomes;
	}

	public void setPhased(boolean phased) {
		this.phased = phased;
	}

	public boolean isPhased() {
		return phased;
	}

	@Override
	public String toString() {
		return "Chromosome: " + getChromosome() + "\n Samples: "
				+ getNoSamples() + "\n Snps: " + getNoSnps() + "\n Chunks: "
				+ getChunks().size();
	}

	@Override
	public String[] getFilenames() {
		return new String[] { getVcfFilename(), getIndexFilename() };
	}

	@Override
	public String getType() {

		if (phased) {

			return "VCF-PHASED";

		} else {

			return "VCF-UNPHASED";

		}
	}

	@Override
	public boolean needsMapping() {
		return false;
	}

}
