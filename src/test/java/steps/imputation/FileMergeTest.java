package steps.imputation;

import genepi.hadoop.command.Command;
import genepi.imputationserver.steps.vcf.MergedVcfFile;
import genepi.imputationserver.util.FileMerger;
import genepi.io.FileUtil;
import genepi.io.text.LineReader;

import java.io.FileInputStream;
import java.io.IOException;

import junit.framework.TestCase;

public class FileMergeTest extends TestCase {

	public static final String TEMP_FOLDER = "temp";

	public static final String INPUT_FILE = "test-data/chr1.phased.vcf";

	public void testBgZipMerge() throws IOException {

		FileUtil.deleteDirectory(TEMP_FOLDER);
		FileUtil.createDirectory(TEMP_FOLDER);

		// splits
		int chunks = FileMerger.splitIntoHeaderAndDataBgZip(INPUT_FILE, TEMP_FOLDER
				+ "/vcf_chunk");

		// merge
		MergedVcfFile vcfFile = new MergedVcfFile(TEMP_FOLDER + "/out.vcf.gz");
		vcfFile.addFile(new FileInputStream(TEMP_FOLDER
				+ "/vcf_chunk.header.vcf.gz"));
		for (int i = 0; i <= chunks; i++) {
			vcfFile.addFile(new FileInputStream(TEMP_FOLDER + "/vcf_chunk_" + i
					+ ".data.vcf.gz"));
		}
		vcfFile.close();

		// compare input and output
		LineReader a = new LineReader(INPUT_FILE);
		LineReader b = new LineReader((TEMP_FOLDER + "/out.vcf.gz"));
		while (a.next()) {
			b.next();
			assertEquals(a.get(), b.get());
		}
		a.close();
		b.close();

		// check tabix compatibility
		Command tabix2 = new Command("files/minimac/bin/tabix");
		tabix2.setSilent(false);
		tabix2.setParams("-f", TEMP_FOLDER + "/out.vcf.gz");
		assertTrue(tabix2.execute() == 0);
		FileUtil.deleteDirectory(TEMP_FOLDER);

	}
	
	public void testGZipMerge() throws IOException {

		FileUtil.deleteDirectory(TEMP_FOLDER);
		FileUtil.createDirectory(TEMP_FOLDER);

		// splits
		int chunks = FileMerger.splitIntoHeaderAndData(INPUT_FILE, TEMP_FOLDER
				+ "/vcf_chunk");

		// merge
		MergedVcfFile vcfFile = new MergedVcfFile(TEMP_FOLDER + "/out.vcf.gz");
		vcfFile.addFile(new FileInputStream(TEMP_FOLDER
				+ "/vcf_chunk.header.vcf.gz"));
		for (int i = 0; i <= chunks; i++) {
			vcfFile.addFile(new FileInputStream(TEMP_FOLDER + "/vcf_chunk_" + i
					+ ".data.vcf.gz"));
		}
		vcfFile.close();

		// compare input and output
		LineReader a = new LineReader(INPUT_FILE);
		LineReader b = new LineReader((TEMP_FOLDER + "/out.vcf.gz"));
		while (a.next()) {
			b.next();
			assertEquals(a.get(), b.get());
		}
		a.close();
		b.close();
		FileUtil.deleteDirectory(TEMP_FOLDER);

	}

}
