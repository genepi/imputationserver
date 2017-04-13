package genepi.imputationserver.steps.vcf;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.zip.GZIPOutputStream;

import htsjdk.samtools.util.BlockCompressedOutputStream;

public class BGzipLineWriter {

	private BufferedWriter bw;

	private boolean first = true;

	public BGzipLineWriter(String filename) throws IOException {
		bw = new BufferedWriter(new OutputStreamWriter(new BlockCompressedOutputStream(new File(filename))));
		first = true;
	}

	public void write(String line) throws IOException {
		if (first) {
			first = false;
		} else {
			bw.newLine();
		}

		bw.write(line);
	}

	public void close() throws IOException {
		bw.close();
	}

}
