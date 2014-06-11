package genepi.minicloudmac.hadoop.validation.io;

import java.util.Set;

public abstract class AbstractPair {

	public abstract int getNoSnps();

	public abstract Set<String> getChromosomes();

	public abstract String getChromosome();

	public abstract String[] getFilenames();

	public abstract int getNoSamples();

	public abstract Set<Integer> getChunks();

	public abstract String getType();

	public abstract boolean needsMapping();

	@Override
	public String toString() {
		return "Chromosome: " + getChromosome() + "\n Samples: "
				+ getNoSamples() + "\n Snps: " + getNoSnps() + "\n Chunks: "
				+ getChunks().size();
	}
}
