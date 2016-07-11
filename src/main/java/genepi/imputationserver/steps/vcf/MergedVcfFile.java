package genepi.imputationserver.steps.vcf;

import htsjdk.samtools.util.BlockCompressedStreamConstants;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;

import org.apache.commons.compress.utils.IOUtils;

public class MergedVcfFile {

	private FileOutputStream output;

	public MergedVcfFile(String filename) throws FileNotFoundException {
		output = new FileOutputStream(filename);
	}

	public void addFile(InputStream input) throws IOException {
		IOUtils.copy(input, output);
		input.close();
	}

	public void close() throws IOException {
		output.write(BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK);
		output.close();
	}

}
