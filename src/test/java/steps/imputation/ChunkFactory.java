package steps.imputation;

import genepi.imputationserver.steps.vcf.VcfChunk;

public class ChunkFactory {

	public static VcfChunk getChunkForGckdTestSnp(){
		// chunk
		VcfChunk chunk = new VcfChunk();
		chunk.setChromosome("20");
		chunk.setStart(20000001);
		chunk.setEnd(30000000);
		chunk.setPhased(true);
		chunk.setVcfFilename("test-data/gckd/gckd-chr20.renamed.vcf.gz");
		return chunk;
	}

}
