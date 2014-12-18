package genepi.imputationserver.steps.vcf;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.compress.utils.IOUtils;

public class MergedVcfFile {

	private FileOutputStream output;

	private boolean firstFile = true;

	// Header flags
	// private static final int FTEXT = 0x01; // Uninteresting for us
	private static final int FHCRC = 0x02;
	private static final int FEXTRA = 0x04;
	private static final int FNAME = 0x08;
	private static final int FCOMMENT = 0x10;
	private static final int FRESERVED = 0xE0;

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
		output.close();
	}

	private boolean skipHeader(InputStream in) throws IOException {

		// Check the magic bytes without a possibility of EOFException.
		int magic0 = in.read();
		int magic1 = in.read();

		// If end of input was reached after decompressing at least
		// one .gz member, we have reached the end of the file successfully.
		if (magic0 == -1) {
			// endOfStream = true;
			return false;
		}

		if (magic0 != 31 || magic1 != 139) {
			throw new IOException("Input is not in the .gz format");
		}

		// Parsing the rest of the header may throw EOFException.
		DataInputStream inData = new DataInputStream(in);
		int method = inData.readUnsignedByte();
		if (method != 8) {
			throw new IOException("Unsupported compression method " + method
					+ " in the .gz header");
		}

		int flg = inData.readUnsignedByte();
		if ((flg & FRESERVED) != 0) {
			throw new IOException("Reserved flags are set in the .gz header");
		}

		inData.readInt(); // mtime, ignored
		inData.readUnsignedByte(); // extra flags, ignored
		inData.readUnsignedByte(); // operating system, ignored

		// Extra field, ignored
		if ((flg & FEXTRA) != 0) {
			int xlen = inData.readUnsignedByte();
			xlen |= inData.readUnsignedByte() << 8;

			// This isn't as efficient as calling in.skip would be,
			// but it's lazier to handle unexpected end of input this way.
			// Most files don't have an extra field anyway.
			while (xlen-- > 0) {
				inData.readUnsignedByte();
			}
		}

		// Original file name, ignored
		if ((flg & FNAME) != 0) {
			readToNull(inData);
		}

		// Comment, ignored
		if ((flg & FCOMMENT) != 0) {
			readToNull(inData);
		}

		// Header "CRC16" which is actually a truncated CRC32 (which isn't
		// as good as real CRC16). I don't know if any encoder implementation
		// sets this, so it's not worth trying to verify it. GNU gzip 1.4
		// doesn't support this field, but zlib seems to be able to at least
		// skip over it.
		if ((flg & FHCRC) != 0) {
			inData.readShort();
		}

		return true;
	}

	private void readToNull(DataInputStream inData) throws IOException {
		while (inData.readUnsignedByte() != 0x00) {
		}
	}
	
	public static void main(String[] args) throws IOException {
		
		String folder = "/Users/lukas/data/lf/1";
		
		MergedVcfFile vcfFile = new MergedVcfFile("test.vcf.gz");
		String[] files = new File(folder).list();
		for (String file: files){
			System.out.println("Read file " + file);
			vcfFile.addFile(new FileInputStream(folder+"/" + file));
		}
		vcfFile.close();
	}

}
