package genepi.imputationserver.steps.vcf;

import genepi.io.FileUtil;

public class VcfChunkOutput extends VcfChunk {

	private String prefix;
	private String imputedVcfFilename;
	private String phasedVcfFilename;
	private String scoreFilename;

	private String infoFilename;

	public VcfChunkOutput(VcfChunk chunk, String outputFolder) {

		prefix = FileUtil.path(outputFolder, chunk.getId());
		imputedVcfFilename = prefix + ".dose.vcf.gz";
		infoFilename = prefix + ".info";
		phasedVcfFilename = prefix + ".phased.vcf.gz";
		scoreFilename = prefix + ".scores.csv";

		setVcfFilename(prefix + ".vcf.gz");
		setChromosome(chunk.getChromosome());
		setStart(chunk.getStart());
		setEnd(chunk.getEnd());
		setPhased(chunk.isPhased());
	}

	public String getPrefix() {
		return prefix;
	}

	public String getInfoFilename() {
		return infoFilename;
	}

	public String getImputedVcfFilename() {
		return imputedVcfFilename;
	}

	public String getPhasedVcfFilename() {
		return phasedVcfFilename;
	}
	
	public String getScoreFilename() {
		return scoreFilename;
	}

}
