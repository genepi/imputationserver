package genepi.imputationserver.steps.vcf;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import net.sf.samtools.util.BlockCompressedStreamConstants;

import org.apache.commons.compress.utils.IOUtils;

public class MergedVcfFile {

	private FileOutputStream output;


	public MergedVcfFile(String filename) throws FileNotFoundException {
		output = new FileOutputStream(filename);
	}

	public void addFile(InputStream input) throws IOException {
		//if (firstFile) {
			IOUtils.copy(input, output);
		//	firstFile = false;
		//} else {
		//	//skipHeader(input);
		//	IOUtils.copy(input, output);
		//}
		input.close();
	}

	public void close() throws IOException {
		output.write(BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK);
		output.close();
	}



}
