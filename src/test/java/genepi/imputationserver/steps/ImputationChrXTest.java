package genepi.imputationserver.steps;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import genepi.imputationserver.BaseTestCase;
import genepi.imputationserver.steps.vcf.VcfFile;
import genepi.imputationserver.steps.vcf.VcfFileUtil;
import genepi.imputationserver.util.WorkflowTestContext;
import genepi.io.FileUtil;
import htsjdk.samtools.util.CloseableIterator;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;
import net.lingala.zip4j.ZipFile;
import net.lingala.zip4j.exception.ZipException;

public class ImputationChrXTest extends BaseTestCase {

	public final int TOTAL_REFPANEL_CHRX_B37 = 1479509;
	public final int TOTAL_REFPANEL_CHRX_B38 = 1077575;
	// public final int SNPS_WITH_R2_BELOW_05 = 6344;

	@Test
	public void testChrXPipelineWithEagle() throws IOException, ZipException {

		// maybe git large files?
		if (!new File(
				"test-data/configs/hapmap-chrX/ref-panels/ALL.chrX.nonPAR.phase1_v3.snps_indels_svs.genotypes.all.noSingleton.recode.bcf")
						.exists()) {
			System.out.println("chrX bcf nonPAR file not available");
			return;
		}

		String configFolder = "test-data/configs/hapmap-chrX";
		String inputFolder = "test-data/data/chrX-unphased";

		File file = new File("test-data/tmp");
		if (file.exists()) {
			FileUtil.deleteDirectory(file);
		}

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, configFolder, "phase1");

		// run qc to create chunkfile
		FastQualityControlMock qcStats = new FastQualityControlMock(configFolder);
		boolean result = run(context, qcStats);

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

		ZipFile zipFile = new ZipFile("test-data/tmp/local/chr_X.zip", PASSWORD.toCharArray());

		zipFile.extractAll("test-data/tmp");

		VcfFile vcfFile = VcfFileUtil.load("test-data/tmp/chrX.dose.vcf.gz", 100000000, false);

		assertEquals("X", vcfFile.getChromosome());

		FileUtil.deleteDirectory(file);

	}

	@Test
	public void testChrXPipelinePhased() throws IOException, ZipException {

		String configFolder = "test-data/configs/hapmap-chrX";
		String inputFolder = "test-data/data/chrX-phased";

		File file = new File("test-data/tmp");
		if (file.exists()) {
			FileUtil.deleteDirectory(file);
		}

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, configFolder,"phase1");

		// run qc to create chunkfile
		FastQualityControlMock qcStats = new FastQualityControlMock(configFolder);
		boolean result = run(context, qcStats);

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

		ZipFile zipFile = new ZipFile("test-data/tmp/local/chr_X.zip", PASSWORD.toCharArray());
		zipFile.extractAll("test-data/tmp");

		VcfFile vcfFile = VcfFileUtil.load("test-data/tmp/chrX.dose.vcf.gz", 100000000, false);

		assertEquals("X", vcfFile.getChromosome());
		assertEquals(26, vcfFile.getNoSamples());
		assertEquals(true, vcfFile.isPhased());
		assertEquals(TOTAL_REFPANEL_CHRX_B37, vcfFile.getNoSnps());

		FileUtil.deleteDirectory(file);

	}

	@Test
	public void testChr23PipelinePhased() throws IOException, ZipException {

		String configFolder = "test-data/configs/hapmap-chrX";
		String inputFolder = "test-data/data/chr23-phased";

		File file = new File("test-data/tmp");
		if (file.exists()) {
			FileUtil.deleteDirectory(file);
		}

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, configFolder,"phase1");

		// run qc to create chunkfile
		FastQualityControlMock qcStats = new FastQualityControlMock(configFolder);
		boolean result = run(context, qcStats);

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

		ZipFile zipFile = new ZipFile("test-data/tmp/local/chr_X.zip", PASSWORD.toCharArray());
		zipFile.extractAll("test-data/tmp");

		VcfFile vcfFile = VcfFileUtil.load("test-data/tmp/chrX.dose.vcf.gz", 100000000, false);

		assertEquals("X", vcfFile.getChromosome());
		assertEquals(26, vcfFile.getNoSamples());
		assertEquals(true, vcfFile.isPhased());
		assertEquals(TOTAL_REFPANEL_CHRX_B37, vcfFile.getNoSnps());

		FileUtil.deleteDirectory(file);

	}

	@Test
	public void testChrXLeaveOneOutPipelinePhased() throws IOException, ZipException {

		// SNP 26963697 from input excluded and imputed!
		// true genotypes:
		// 1,1|1,1|1,1|1,1,1|1,1,1|1,1|1,1,0,1|1,1|0,1,1,1,1,1,1|1,1,1|1,1|1,1|1,1|1,1|1,1|0,

		String configFolder = "test-data/configs/hapmap-chrX";
		String inputFolder = "test-data/data/chrX-phased-loo";

		File file = new File("test-data/tmp");
		if (file.exists()) {
			FileUtil.deleteDirectory(file);
		}

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, configFolder,"phase1");

		// run qc to create chunkfile
		FastQualityControlMock qcStats = new FastQualityControlMock(configFolder);
		boolean result = run(context, qcStats);

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

		ZipFile zipFile = new ZipFile("test-data/tmp/local/chr_X.zip", PASSWORD.toCharArray());
		zipFile.extractAll("test-data/tmp");

		VcfFile vcfFile = VcfFileUtil.load("test-data/tmp/chrX.dose.vcf.gz", 100000000, false);

		VCFFileReader vcfReader = new VCFFileReader(new File(vcfFile.getVcfFilename()), false);

		CloseableIterator<VariantContext> it = vcfReader.iterator();

		while (it.hasNext()) {

			VariantContext line = it.next();

			if (line.getStart() == 26963697) {
				assertEquals(2, line.getHetCount());
				assertEquals(1, line.getHomRefCount());
				assertEquals(23, line.getHomVarCount());

			}
		}

		vcfReader.close();

		FileUtil.deleteDirectory(file);

	}

	@Test
	public void testChrXPipelineWithPhasedHg38() throws IOException, ZipException {

		String configFolder = "test-data/configs/hapmap-chrX-hg38";
		String inputFolder = "test-data/data/chrX-phased";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, configFolder,"hapmap2");

		// run qc to create chunkfile
		FastQualityControlMock qcStats = new FastQualityControlMock(configFolder);
		boolean result = run(context, qcStats);

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

		ZipFile zipFile = new ZipFile("test-data/tmp/local/chr_X.zip", PASSWORD.toCharArray());
		zipFile.extractAll("test-data/tmp");

		VcfFile vcfFile = VcfFileUtil.load("test-data/tmp/chrX.dose.vcf.gz", 100000000, false);

		assertEquals("X", vcfFile.getChromosome());
		assertEquals(26, vcfFile.getNoSamples());
		assertEquals(true, vcfFile.isPhased());
		assertEquals(TOTAL_REFPANEL_CHRX_B38, vcfFile.getNoSnps());

		FileUtil.deleteDirectory("test-data/tmp");

	}

	@Test
	public void testChrXPipelineWithEagleHg38() throws IOException, ZipException {

		String configFolder = "test-data/configs/hapmap-chrX-hg38";
		String inputFolder = "test-data/data/chrX-unphased";

		// maybe git large files?
		if (!new File(
				"test-data/configs/hapmap-chrX-hg38/ref-panels/ALL.X.nonPAR.phase1_v3.snps_indels_svs.genotypes.all.noSingleton.recode.hg38.bcf")
						.exists()) {
			System.out.println("chrX bcf nonPAR file not available");
			return;
		}

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder,configFolder, "hapmap2");

		// run qc to create chunkfile
		FastQualityControlMock qcStats = new FastQualityControlMock(configFolder);
		boolean result = run(context, qcStats);

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

		ZipFile zipFile = new ZipFile("test-data/tmp/local/chr_X.zip", PASSWORD.toCharArray());
		zipFile.extractAll("test-data/tmp");

		VcfFile vcfFile = VcfFileUtil.load("test-data/tmp/chrX.dose.vcf.gz", 100000000, false);

		assertEquals("X", vcfFile.getChromosome());
		assertEquals(26, vcfFile.getNoSamples());
		assertEquals(true, vcfFile.isPhased());
		assertEquals(TOTAL_REFPANEL_CHRX_B38, vcfFile.getNoSnps());

		FileUtil.deleteDirectory("test-data/tmp");

	}
	
	@Test
	public void testPipelineChrXWithEaglePhasingOnly() throws IOException, ZipException {
		
		if (!new File(
				"test-data/configs/hapmap-chrX-hg38/ref-panels/ALL.X.nonPAR.phase1_v3.snps_indels_svs.genotypes.all.noSingleton.recode.hg38.bcf")
						.exists()) {
			System.out.println("chrX bcf nonPAR file not available");
			return;
		}


		String configFolder = "test-data/configs/hapmap-chrX";
		String inputFolder = "test-data/data/chrX-unphased";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, configFolder,"phase1");
		
		context.setInput("mode", "phasing");

		// run qc to create chunkfile
		FastQualityControlMock qcStats = new FastQualityControlMock(configFolder);
		boolean result = run(context, qcStats);

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

		ZipFile zipFile = new ZipFile("test-data/tmp/local/chr_X.zip", PASSWORD.toCharArray());
		zipFile.extractAll("test-data/tmp");

		VcfFile vcfFile = VcfFileUtil.load("test-data/tmp/chrX.phased.vcf.gz", 100000000, false);
		
		assertEquals(true, vcfFile.isPhased());
		
		VCFFileReader vcfReader = new VCFFileReader(new File(vcfFile.getVcfFilename()), false);
		
		CloseableIterator<VariantContext> it = vcfReader.iterator();

		while (it.hasNext()) {

			VariantContext line = it.next();

			if (line.getStart() == 44322058) {
				assertEquals("A", line.getGenotype("HG00096").getGenotypeString());
				System.out.println(line.getGenotype("HG00097").getGenotypeString());
				assertEquals("A|A", line.getGenotype("HG00097").getGenotypeString());
			}
		}
		
		vcfReader.close();

		FileUtil.deleteDirectory("test-data/tmp");

	}

}
