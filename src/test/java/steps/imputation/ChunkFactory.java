package steps.imputation;

import genepi.imputationserver.steps.vcf.VcfChunk;

public class ChunkFactory {

	public static VcfChunk getChunkForSmall50() {
		// chunk
		VcfChunk chunk = new VcfChunk();
		chunk.setChromosome("1");
		chunk.setStart(534247);
		chunk.setEnd(1798916);
		chunk.setPhased(true);
		chunk.setVcfFilename("test-data/small-50/minimac_test.50.vcf.gz");
		return chunk;
	}

	public static VcfChunk getChunkForGckdTestSnp() {
		// chunk
		VcfChunk chunk = new VcfChunk();
		chunk.setChromosome("20");
		chunk.setStart(20000001);
		chunk.setEnd(20050000);
		// chunk.setEnd(30000000);
		chunk.setPhased(true);
		chunk.setVcfFilename("test-data/gckd/gckd-chr20.renamed.vcf");
		return chunk;
	}

	public static VcfChunk getChunkForMetSim() {
		// chunk
		VcfChunk chunk = new VcfChunk();
		chunk.setChromosome("20");
		chunk.setStart(1);
		chunk.setEnd(10000000);
		chunk.setPhased(true);
		chunk.setVcfFilename("test-data/metsim/metsim_omniexp.chr20.vcf.gz");
		return chunk;
	}

}
