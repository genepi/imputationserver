package genepi.imputationserver.steps.vcf.sort;

public class VcfLine {

	private String data;

	private String contig;

	private int position;

	public VcfLine(String line) {
		String tiles[] = line.split("\t", 3);
		this.contig = tiles[0];
		this.position = Integer.parseInt(tiles[1]);
		this.data = tiles[2];
	}

	public String getLine() {
		return this.contig + "\t" + this.position + "\t" + this.data;
	}

	public int getPosition() {
		return position;
	}

	public void setPosition(int position) {
		this.position = position;
	}

	public void setContig(String contig) {
		this.contig = contig;
	}

	public String getContig() {
		return contig;
	}

}
