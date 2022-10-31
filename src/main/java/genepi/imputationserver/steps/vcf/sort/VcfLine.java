package genepi.imputationserver.steps.vcf.sort;

public class VcfLine {

	private String data;

	private String contig;

	private int position;

	private String reference;

	private String alternate;

	private String id;

	public VcfLine(String line) {
		String tiles[] = line.split("\t", 6);
		this.contig = tiles[0];
		this.position = Integer.parseInt(tiles[1]);
		this.id = tiles[2];
		this.reference = tiles[3];
		this.alternate = tiles[4];
		this.data = tiles[5];
	}

	public String getLine() {
		StringBuilder builder = new StringBuilder();
		builder.append(this.contig);
		builder.append("\t");
		builder.append(this.position);
		builder.append("\t");
		builder.append(id);
		builder.append("\t");
		builder.append(reference);
		builder.append("\t");
		builder.append(alternate);
		builder.append("\t");
		builder.append(this.data);
		return builder.toString();
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

	public String getReference() {
		return reference;
	}

	public String getAlternate() {
		return alternate;
	}

	public void setReference(String reference) {
		this.reference = reference;
	}

	public void setAlternate(String alternate) {
		this.alternate = alternate;
	}

	public String getId() {
		return id;
	}

}
