package genepi.minicloudmac.hadoop.validation.io.vcf;

import java.text.DecimalFormat;

public class VcfChunk {

	private String chromosome;

	private String vcfFilename;

	private String indexFilename;

	private boolean phased = true;

	private int start;

	private int end;

	private DecimalFormat nf = new DecimalFormat("#0000000000");

	public VcfChunk(){
		
	}
	
	public VcfChunk(String line) {
		String[] tiles = line.split("\t");

		chromosome = tiles[0];
		start = Integer.parseInt(tiles[1]);
		end = Integer.parseInt(tiles[2]);
		phased = tiles[3].equals("VCF-PHASED");
		vcfFilename = tiles[4];
		indexFilename = tiles[5];
	}

	public String getChromosome() {
		return chromosome;
	}

	public void setChromosome(String chromosome) {
		this.chromosome = chromosome;
	}

	public String getVcfFilename() {
		return vcfFilename;
	}

	public void setVcfFilename(String vcfFilename) {
		this.vcfFilename = vcfFilename;
	}

	public String getIndexFilename() {
		return indexFilename;
	}

	public void setIndexFilename(String indexFilename) {
		this.indexFilename = indexFilename;
	}

	public boolean isPhased() {
		return phased;
	}

	public void setPhased(boolean phased) {
		this.phased = phased;
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

	public String toString() {
		return getId();
	}
	
	public String serialize() {
		return chromosome + "\t" + start + "\t" + end + "\t"
				+ (phased ? "VCF-PHASED" : "VCF-UNPHASED") + "\t" + vcfFilename
				+ "\t" + indexFilename;
	}

	public String getId() {
		return "chunk_" + chromosome + "_" + nf.format(start) + "_"
				+ nf.format(end);

	}

}
