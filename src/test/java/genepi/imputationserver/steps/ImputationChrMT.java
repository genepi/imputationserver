package genepi.imputationserver.steps;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import genepi.imputationserver.BaseTestCase;
import genepi.imputationserver.steps.vcf.VcfFile;
import genepi.imputationserver.steps.vcf.VcfFileUtil;
import genepi.imputationserver.util.WorkflowTestContext;
import genepi.io.FileUtil;
import net.lingala.zip4j.ZipFile;
import net.lingala.zip4j.exception.ZipException;

public class ImputationChrMT extends BaseTestCase {

	@Test
	public void testChrMTPipeline() throws IOException, ZipException {

		String configFolder = "test-data/configs/phylotree-chrMT";
		String inputFolder = "test-data/data/chrMT";

		File file = new File("test-data/tmp");
		if (file.exists()) {
			FileUtil.deleteDirectory(file);
		}

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, configFolder, "phylotree");

		// create step instance
		InputValidation inputValidation = new InputValidationMock(configFolder);

		// run and test
		boolean result = run(context, inputValidation);
		assertTrue(result);

		// run qc to create chunkfile
		FastQualityControlMock qualityControl = new FastQualityControlMock(configFolder);
		result = run(context, qualityControl);

		assertTrue(result);

		// add panel to hdfs
		importRefPanel(FileUtil.path(configFolder, "ref-panels"));
		// importMinimacMap("test-data/B38_MAP_FILE.map");
		importBinaries("files/bin");

		// run imputation
		ImputationMinimac3Mock imputation = new ImputationMinimac3Mock(configFolder);
		result = run(context, imputation);
		assertTrue(result);

		// run export
		CompressionEncryptionMock export = new CompressionEncryptionMock("files");
		result = run(context, export);
		assertTrue(result);

		ZipFile zipFile = new ZipFile("test-data/tmp/local/chr_MT.zip", PASSWORD.toCharArray());

		zipFile.extractAll("test-data/tmp");

		VcfFile vcfFile = VcfFileUtil.load("test-data/tmp/chrMT.dose.vcf.gz", 100000000, false);

		assertEquals("MT", vcfFile.getChromosome());
		assertEquals(5435, vcfFile.getNoSamples());
		FileUtil.deleteDirectory(file);

	}

}
