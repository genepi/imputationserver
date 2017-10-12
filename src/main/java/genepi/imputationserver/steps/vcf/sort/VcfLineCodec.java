package genepi.imputationserver.steps.vcf.sort;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;

import htsjdk.samtools.util.RuntimeIOException;
import htsjdk.samtools.util.SortingCollection.Codec;

public class VcfLineCodec implements Codec<VcfLine> {

	private PrintStream outputStream = null;
	private BufferedReader inputReader = null;

	@Override
	public void setOutputStream(final OutputStream stream) {
		this.outputStream = new PrintStream(stream);
	}

	@Override
	public void setInputStream(final InputStream stream) {
		this.inputReader = new BufferedReader(new InputStreamReader(stream));
	}

	@Override
	public void encode(VcfLine vcfLine) {
		this.outputStream.println(vcfLine.getLine());
	}

	@Override
	public VcfLine decode() {
		try {
			final String line;
			return ((line = inputReader.readLine()) != null) ? new VcfLine(line) : null;
		} catch (final IOException ioe) {
			throw new RuntimeIOException(
					"Could not decode/read a VCF record for a sorting collection: " + ioe.getMessage(), ioe);
		}

	}

	@Override
	public Codec<VcfLine> clone() {
		return new VcfLineCodec();
	}

}
