package genepi.minicloudmac.hadoop.validation.io.vcf;

import genepi.io.FileUtil;

public class VcfChunkOutput extends VcfChunk {

	private String prefix;
	private String bimFilename;
	private String bedFilename;
	private String famFilename;
	private String hapsFilename;
	private String hapsCleanFilename;
	private String hapFilename;
	private String sampleFilename;
	private String snpsFilename;
	private String doseFilename;
	private String infoFilename;
	private String infoFixedFilename;

	public VcfChunkOutput(VcfChunk chunk, String outputFolder) {

		prefix = FileUtil.path(outputFolder, chunk.getId());
		bimFilename = prefix + ".bim";
		bedFilename = prefix + ".bed";
		famFilename = prefix + ".fam";
		hapsFilename = prefix + ".haps";
		hapsCleanFilename = hapsFilename + ".clean";
		hapFilename = prefix + ".hap";
		sampleFilename = prefix + ".sample";
		snpsFilename = prefix + ".snps";
		doseFilename = prefix + ".dose";
		infoFilename = prefix + ".info";
		infoFixedFilename = infoFilename + ".fixed";

		setVcfFilename(prefix + ".vcf");
		setChromosome(chunk.getChromosome());
		setStart(chunk.getStart());
		setEnd(chunk.getEnd());
		setPhased(chunk.isPhased());
		setIndexFilename(chunk.getIndexFilename());
	}

	public String getPrefix() {
		return prefix;
	}

	public String getBimFilename() {
		return bimFilename;
	}

	public String getFamFilename() {
		return famFilename;
	}

	public String getHapsFilename() {
		return hapsFilename;
	}

	public String getHapFilename() {
		return hapFilename;
	}
	
	public String getHapsCleanFilename() {
		return hapsCleanFilename;
	}

	public String getSampleFilename() {
		return sampleFilename;
	}

	public String getSnpsFilename() {
		return snpsFilename;
	}

	public String getDoseFilename() {
		return doseFilename;
	}

	public String getInfoFilename() {
		return infoFilename;
	}

	public String getInfoFixedFilename() {
		return infoFixedFilename;
	}

	public String getBedFilename() {
		return bedFilename;
	}

}
