package genepi.imputationserver.steps;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import cloudgene.sdk.internal.WorkflowStep;
import genepi.hadoop.HdfsUtil;
import genepi.imputationserver.steps.vcf.VcfFile;
import genepi.imputationserver.steps.vcf.VcfFileUtil;
import genepi.imputationserver.util.TestCluster;
import genepi.imputationserver.util.WorkflowTestContext;
import genepi.io.FileUtil;
import genepi.io.text.LineReader;
import net.lingala.zip4j.ZipFile;
import net.lingala.zip4j.exception.ZipException;

public class ImputationTest {

	public static final boolean VERBOSE = true;

	public static final String BINARIES_HDFS = "binaries";

	public static final String PASSWORD = "random-pwd";

	public final int TOTAL_REFPANEL_CHR20_B37 = 63402;
	public final int TOTAL_REFPANEL_CHR20_B38 = 63384;
	public final int ONLY_IN_INPUT = 78;
	public final int TOTAL_SNPS_INPUT = 7824;
	public final int SNPS_MONOMORPHIC = 11;
	// public final int SNPS_WITH_R2_BELOW_05 = 6344;

	@BeforeClass
	public static void setUp() throws Exception {
		TestCluster.getInstance().start();
	}

	@AfterClass
	public static void tearDown() throws Exception {
		TestCluster.getInstance().stop();
	}

	@Test
	public void testPipelineWithPhased() throws IOException, ZipException {

		String configFolder = "test-data/configs/hapmap-chr20";
		String inputFolder = "test-data/data/chr20-phased";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, "hapmap2");

		// run qc to create chunkfile
		QcStatisticsMock qcStats = new QcStatisticsMock(configFolder);
		boolean result = run(context, qcStats);

		assertTrue(result);
		assertTrue(context.hasInMemory("Remaining sites in total: 7,735"));

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

		ZipFile zipFile = new ZipFile("test-data/tmp/local/chr_20.zip", PASSWORD.toCharArray());
		zipFile.extractAll("test-data/tmp");

		VcfFile file = VcfFileUtil.load("test-data/tmp/chr20.dose.vcf.gz", 100000000, false);

		assertEquals("20", file.getChromosome());
		assertEquals(51, file.getNoSamples());
		assertEquals(true, file.isPhased());
		assertEquals(TOTAL_REFPANEL_CHR20_B37 + ONLY_IN_INPUT, file.getNoSnps());

		// FileUtil.deleteDirectory("test-data/tmp");

	}

	@Test
	public void testPipelineWithPhasedAndEmptyPhasing() throws IOException, ZipException {

		String configFolder = "test-data/configs/hapmap-chr20";
		String inputFolder = "test-data/data/chr20-phased";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, "hapmap2");

		// run qc to create chunkfile
		QcStatisticsMock qcStats = new QcStatisticsMock(configFolder);
		boolean result = run(context, qcStats);

		assertTrue(result);
		assertTrue(context.hasInMemory("Remaining sites in total: 7,735"));

		// add panel to hdfs
		importRefPanel(FileUtil.path(configFolder, "ref-panels"));
		// importMinimacMap("test-data/B38_MAP_FILE.map");
		importBinaries("files/bin");

		// run imputation
		ImputationMinimac3Mock imputation = new ImputationMinimac3Mock(configFolder);
		result = run(context, imputation);
		assertTrue(result);

		FileUtil.deleteDirectory("test-data/tmp");

	}

	@Test
	public void testPipelineWithPhasedAndNoPhasingSelected() throws IOException, ZipException {

		String configFolder = "test-data/configs/hapmap-chr20";
		String inputFolder = "test-data/data/chr20-phased";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, "hapmap2");

		// run qc to create chunkfile
		QcStatisticsMock qcStats = new QcStatisticsMock(configFolder);
		boolean result = run(context, qcStats);

		assertTrue(result);
		assertTrue(context.hasInMemory("Remaining sites in total: 7,735"));

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

		ZipFile zipFile = new ZipFile("test-data/tmp/local/chr_20.zip", PASSWORD.toCharArray());
		zipFile.extractAll("test-data/tmp");

		VcfFile file = VcfFileUtil.load("test-data/tmp/chr20.dose.vcf.gz", 100000000, false);

		assertEquals("20", file.getChromosome());
		assertEquals(51, file.getNoSamples());
		assertEquals(true, file.isPhased());
		assertEquals(TOTAL_REFPANEL_CHR20_B37 + ONLY_IN_INPUT, file.getNoSnps());

		FileUtil.deleteDirectory("test-data/tmp");

	}

	@Test
	public void testPipelineWidthInvalidHttpUrl() throws IOException, ZipException {

		String configFolder = "test-data/configs/hapmap-chr1";
		String inputFolder = "https://imputationserver.sph.umich.edu/invalid-url/downloads/hapmap300.chr1.recode.vcf.gz";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, "hapmap2");

		// create step instance
		InputValidation inputValidation = new InputValidationMock(configFolder);

		// run and test
		boolean result = run(context, inputValidation);

		// check if step is failed
		assertEquals(false, result);

		FileUtil.deleteDirectory("test-data/tmp");

	}

	@Test
	public void testPipelineWidthInvalidFileFormat() throws IOException, ZipException {

		String configFolder = "test-data/configs/hapmap-chr1";
		String inputFolder = "https://imputationserver.sph.umich.edu/static/images/impute.png";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, "hapmap2");

		// create step instance
		InputValidation inputValidation = new InputValidationMock(configFolder);

		// run and test
		boolean result = run(context, inputValidation);

		// check if step is failed
		assertEquals(false, result);

		FileUtil.deleteDirectory("test-data/tmp");

	}

	@Test
	public void testPipelineWithHttpUrl() throws IOException, ZipException {

		String configFolder = "test-data/configs/hapmap-chr1";
		String inputFolder = "https://imputationserver.sph.umich.edu/static/downloads/hapmap300.chr1.recode.vcf.gz";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, "hapmap2");

		// create step instance
		InputValidation inputValidation = new InputValidationMock(configFolder);

		// run and test
		boolean result = run(context, inputValidation);

		// check if step is failed
		assertEquals(true, result);

		// run qc to create chunkfile
		QcStatisticsMock qcStats = new QcStatisticsMock(configFolder);
		result = run(context, qcStats);

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

		ZipFile zipFile = new ZipFile("test-data/tmp/local/chr_1.zip", PASSWORD.toCharArray());
		zipFile.extractAll("test-data/tmp");

		VcfFile file = VcfFileUtil.load("test-data/tmp/chr1.dose.vcf.gz", 100000000, false);

		assertEquals("1", file.getChromosome());
		assertEquals(60, file.getNoSamples());
		assertEquals(true, file.isPhased());

		FileUtil.deleteDirectory("test-data/tmp");

	}

	/*
	 * @Test public void testPipelineWithS3() throws IOException, ZipException {
	 * 
	 * String configFolder = "test-data/configs/hapmap-chr1"; String inputFolder =
	 * "s3://imputationserver-aws-testdata/test-s3/hapmap300.chr1.recode.vcf.gz";
	 * 
	 * // create workflow context WorkflowTestContext context =
	 * buildContext(inputFolder, "hapmap2");
	 * 
	 * // create step instance InputValidation inputValidation = new
	 * InputValidationMock(configFolder);
	 * 
	 * // run and test boolean result = run(context, inputValidation);
	 * 
	 * // check if step is failed assertEquals(true, result);
	 * 
	 * // run qc to create chunkfile QcStatisticsMock qcStats = new
	 * QcStatisticsMock(configFolder); result = run(context, qcStats);
	 * 
	 * // add panel to hdfs importRefPanel(FileUtil.path(configFolder,
	 * "ref-panels")); // importMinimacMap("test-data/B38_MAP_FILE.map");
	 * importBinaries("files/bin");
	 * 
	 * // run imputation ImputationMinimac3Mock imputation = new
	 * ImputationMinimac3Mock(configFolder); result = run(context, imputation);
	 * assertTrue(result);
	 * 
	 * // run export CompressionEncryptionMock export = new
	 * CompressionEncryptionMock("files"); result = run(context, export);
	 * assertTrue(result);
	 * 
	 * ZipFile zipFile = new ZipFile("test-data/tmp/local/chr_1.zip"); if
	 * (zipFile.isEncrypted()) { zipFile.setPassword(PASSWORD); }
	 * zipFile.extractAll("test-data/tmp");
	 * 
	 * VcfFile file = VcfFileUtil.load("test-data/tmp/chr1.dose.vcf.gz", 100000000,
	 * false);
	 * 
	 * assertEquals("1", file.getChromosome()); assertEquals(60,
	 * file.getNoSamples()); assertEquals(true, file.isPhased());
	 * 
	 * FileUtil.deleteDirectory("test-data/tmp");
	 * 
	 * }
	 */

	/*
	 * @Test public void testPipelineWithSFTP() throws IOException, ZipException,
	 * InterruptedException {
	 * 
	 * TestSFTPServer server = new TestSFTPServer("test-data/data");
	 * 
	 * String configFolder = "test-data/configs/hapmap-chr20"; String inputFolder =
	 * "sftp://localhost:8001/" + new
	 * File("test-data/data/chr20-phased").getAbsolutePath() + ";" +
	 * TestSFTPServer.USERNAME + ";" + TestSFTPServer.PASSWORD;
	 * 
	 * // create workflow context WorkflowTestContext context =
	 * buildContext(inputFolder, "hapmap2");
	 * 
	 * // create step instance InputValidation inputValidation = new
	 * InputValidationMock(configFolder);
	 * 
	 * // run and test boolean result = run(context, inputValidation);
	 * 
	 * // check if step is failed assertEquals(true, result);
	 * 
	 * // run qc to create chunkfile QcStatisticsMock qcStats = new
	 * QcStatisticsMock(configFolder); result = run(context, qcStats);
	 * 
	 * // add panel to hdfs importRefPanel(FileUtil.path(configFolder,
	 * "ref-panels")); // importMinimacMap("test-data/B38_MAP_FILE.map");
	 * importBinaries("files/bin");
	 * 
	 * // run imputation ImputationMinimac3Mock imputation = new
	 * ImputationMinimac3Mock(configFolder); result = run(context, imputation);
	 * assertTrue(result);
	 * 
	 * // run export CompressionEncryptionMock export = new
	 * CompressionEncryptionMock("files"); result = run(context, export);
	 * assertTrue(result);
	 * 
	 * ZipFile zipFile = new ZipFile("test-data/tmp/local/chr_20.zip"); if
	 * (zipFile.isEncrypted()) { zipFile.setPassword(PASSWORD); }
	 * zipFile.extractAll("test-data/tmp");
	 * 
	 * VcfFile file = VcfFileUtil.load("test-data/tmp/chr20.dose.vcf.gz", 100000000,
	 * false);
	 * 
	 * assertEquals("20", file.getChromosome()); assertEquals(51,
	 * file.getNoSamples()); assertEquals(true, file.isPhased());
	 * assertEquals(TOTAL_REFPANEL_CHR20_B37 + ONLY_IN_INPUT, file.getNoSnps());
	 * 
	 * FileUtil.deleteDirectory("test-data/tmp");
	 * 
	 * server.stop();
	 * 
	 * }
	 * 
	 * @Test public void testPipelineWithWrongSFTPCredentials() throws IOException,
	 * ZipException, InterruptedException {
	 * 
	 * TestSFTPServer server = new TestSFTPServer("test-data/data");
	 * 
	 * String configFolder = "test-data/configs/hapmap-chr20"; String inputFolder =
	 * "sftp://localhost:8001/" + new File("data/chr20-phased").getAbsolutePath() +
	 * ";" + "WRONG_USERNAME" + ";" + TestSFTPServer.PASSWORD;
	 * 
	 * // create workflow context WorkflowTestContext context =
	 * buildContext(inputFolder, "hapmap2");
	 * 
	 * // create step instance InputValidation inputValidation = new
	 * InputValidationMock(configFolder);
	 * 
	 * // run and test boolean result = run(context, inputValidation);
	 * 
	 * // check if step is failed assertEquals(false, result);
	 * 
	 * server.stop();
	 * 
	 * }
	 */

	@Test
	public void testPipelineWithEagle() throws IOException, ZipException {

		String configFolder = "test-data/configs/hapmap-chr20";
		String inputFolder = "test-data/data/chr20-unphased";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, "hapmap2");

		// run qc to create chunkfile
		QcStatisticsMock qcStats = new QcStatisticsMock(configFolder);
		boolean result = run(context, qcStats);

		assertTrue(result);
		assertTrue(context.hasInMemory("Remaining sites in total: 7,735"));

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

		ZipFile zipFile = new ZipFile("test-data/tmp/local/chr_20.zip", PASSWORD.toCharArray());
		zipFile.extractAll("test-data/tmp");

		VcfFile file = VcfFileUtil.load("test-data/tmp/chr20.dose.vcf.gz", 100000000, false);

		assertEquals("20", file.getChromosome());
		assertEquals(51, file.getNoSamples());
		assertEquals(true, file.isPhased());
		assertEquals(TOTAL_REFPANEL_CHR20_B37, file.getNoSnps());

		int snpInInfo = getLineCount("test-data/tmp/chr20.info.gz") - 1;
		assertEquals(snpInInfo, file.getNoSnps());

		FileUtil.deleteDirectory("test-data/tmp");

	}

	@Test
	public void testPipelineWithEaglePhasingOnlyWithPhasedData() throws IOException, ZipException {

		String configFolder = "test-data/configs/hapmap-chr20";
		String inputFolder = "test-data/data/chr20-phased";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, "hapmap2");
		
		context.setInput("mode", "phasing");

		// run qc to create chunkfile
		QcStatisticsMock qcStats = new QcStatisticsMock(configFolder);
		boolean result = run(context, qcStats);

		assertTrue(result);
		assertTrue(context.hasInMemory("Remaining sites in total: 7,735"));

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

		ZipFile zipFile = new ZipFile("test-data/tmp/local/chr_20.zip", PASSWORD.toCharArray());
		zipFile.extractAll("test-data/tmp");

		VcfFile file = VcfFileUtil.load("test-data/tmp/chr20.phased.vcf.gz", 100000000, false);

		assertEquals("20", file.getChromosome());
		assertEquals(51, file.getNoSamples());
		assertEquals(true, file.isPhased());
		assertEquals(TOTAL_SNPS_INPUT - SNPS_MONOMORPHIC, file.getNoSnps());

		FileUtil.deleteDirectory("test-data/tmp");

	}
	
	@Test
	public void testPipelineWithEaglePhasingOnly() throws IOException, ZipException {

		String configFolder = "test-data/configs/hapmap-chr20";
		String inputFolder = "test-data/data/chr20-unphased";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, "hapmap2");

		context.setInput("mode", "phasing");

		// run qc to create chunkfile
		QcStatisticsMock qcStats = new QcStatisticsMock(configFolder);
		boolean result = run(context, qcStats);

		assertTrue(result);
		assertTrue(context.hasInMemory("Remaining sites in total: 7,735"));

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

		ZipFile zipFile = new ZipFile("test-data/tmp/local/chr_20.zip", PASSWORD.toCharArray());
		zipFile.extractAll("test-data/tmp");

		VcfFile file = VcfFileUtil.load("test-data/tmp/chr20.phased.vcf.gz", 100000000, false);

		assertEquals("20", file.getChromosome());
		assertEquals(51, file.getNoSamples());
		assertEquals(true, file.isPhased());
		assertEquals(TOTAL_SNPS_INPUT - SNPS_MONOMORPHIC - ONLY_IN_INPUT, file.getNoSnps());

		FileUtil.deleteDirectory("test-data/tmp");

	}
	
	@Test
	public void testPipelineWithEaglePhasingOnlyHg38() throws IOException, ZipException {

		String configFolder = "test-data/configs/hapmap-chr20-hg38";
		String inputFolder = "test-data/data/chr20-unphased-hg38";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, "hapmap2");
		context.setInput("build", "hg38");
		context.setInput("mode", "phasing");

		// run qc to create chunkfile
		QcStatisticsMock qcStats = new QcStatisticsMock(configFolder);
		boolean result = run(context, qcStats);

		assertTrue(result);
		assertTrue(context.hasInMemory("Remaining sites in total: 7,735"));

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

		ZipFile zipFile = new ZipFile("test-data/tmp/local/chr_20.zip", PASSWORD.toCharArray());
		zipFile.extractAll("test-data/tmp");

		VcfFile file = VcfFileUtil.load("test-data/tmp/chr20.phased.vcf.gz", 100000000, false);

		assertEquals("20", file.getChromosome());
		assertEquals(51, file.getNoSamples());
		assertEquals(true, file.isPhased());
		assertEquals(TOTAL_SNPS_INPUT - SNPS_MONOMORPHIC - ONLY_IN_INPUT, file.getNoSnps());

		FileUtil.deleteDirectory("test-data/tmp");

	}

	@Test
	public void testPipelineWithEagleAndR2Filter() throws IOException, ZipException {

		String configFolder = "test-data/configs/hapmap-chr20";
		String inputFolder = "test-data/data/chr20-unphased";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, "hapmap2");
		context.setInput("r2Filter", "0.5");

		// run qc to create chunkfile
		QcStatisticsMock qcStats = new QcStatisticsMock(configFolder);
		boolean result = run(context, qcStats);

		assertTrue(result);
		assertTrue(context.hasInMemory("Remaining sites in total: 7,735"));

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

		ZipFile zipFile = new ZipFile("test-data/tmp/local/chr_20.zip", PASSWORD.toCharArray());
		zipFile.extractAll("test-data/tmp");

		VcfFile file = VcfFileUtil.load("test-data/tmp/chr20.dose.vcf.gz", 100000000, false);

		assertEquals("20", file.getChromosome());
		assertEquals(51, file.getNoSamples());
		assertEquals(true, file.isPhased());

		// TODO: update SNPS_WITH_R2_BELOW_05
		assertTrue(TOTAL_REFPANEL_CHR20_B37 > file.getNoSnps());

		int snpInInfo = getLineCount("test-data/tmp/chr20.info.gz") - 1;
		assertEquals(snpInInfo, file.getNoSnps());

		FileUtil.deleteDirectory("test-data/tmp");

	}

	private int getLineCount(String filename) throws IOException {
		LineReader reader = new LineReader(filename);
		int lines = 0;
		while (reader.next()) {
			lines++;
		}
		return lines;
	}

	private boolean checkAmountOfColumns(String filename, int tabs) throws IOException {
		LineReader reader = new LineReader(filename);
		while (reader.next()) {

			String line = reader.get();

			if (line.split("\t").length > tabs) {
				return false;
			}

		}

		return true;
	}

	private boolean checkSortPositionInfo(String filename) throws IOException {

		LineReader reader = new LineReader(filename);
		int pos = -1;
		while (reader.next()) {

			String line = reader.get();

			if (!line.startsWith("SNP")) {
				String snp = line.split("\t")[0];
				if (Integer.valueOf(snp.split(":")[1]) <= pos) {
					return false;
				}
				pos = Integer.valueOf(snp.split(":")[1]);
			}

		}

		return true;
	}

	@Test
	public void testPipelineWithEmptyPhasing() throws IOException, ZipException {

		String configFolder = "test-data/configs/hapmap-chr20";
		String inputFolder = "test-data/data/chr20-unphased";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, "hapmap2");

		// create step instance
		InputValidation inputValidation = new InputValidationMock(configFolder);

		// run and test
		boolean result = run(context, inputValidation);

		assertFalse(result);
	}

	@Test
	public void testCompareInfoAndDosageSize() throws IOException, ZipException {

		String configFolder = "test-data/configs/hapmap-chr20";
		String inputFolder = "test-data/data/chr20-unphased";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, "hapmap2");

		// run qc to create chunkfile
		QcStatisticsMock qcStats = new QcStatisticsMock(configFolder);
		boolean result = run(context, qcStats);

		assertTrue(result);
		assertTrue(context.hasInMemory("Remaining sites in total: 7,735"));

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

		ZipFile zipFile = new ZipFile("test-data/tmp/local/chr_20.zip", PASSWORD.toCharArray());
		zipFile.extractAll("test-data/tmp");

		VcfFile file = VcfFileUtil.load("test-data/tmp/chr20.dose.vcf.gz", 100000000, false);

		assertEquals(true, checkAmountOfColumns("test-data/tmp/chr20.info.gz", 13));

		assertEquals(true, checkSortPositionInfo("test-data/tmp/chr20.info.gz"));

		assertEquals("20", file.getChromosome());
		assertEquals(51, file.getNoSamples());
		assertEquals(true, file.isPhased());
		assertEquals(TOTAL_REFPANEL_CHR20_B37, file.getNoSnps());

		// subtract header
		int infoCount = getLineCount("test-data/tmp/chr20.info.gz");
		assertEquals(infoCount - 1, file.getNoSnps());
		FileUtil.deleteDirectory("test-data/tmp");

	}

	@Test
	public void testWriteTypedSitesOnly() throws IOException, ZipException {

		String configFolder = "test-data/configs/hapmap-chr20";
		String inputFolder = "test-data/data/chr20-unphased";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, "hapmap2");

		// run qc to create chunkfile
		QcStatisticsMock qcStats = new QcStatisticsMock(configFolder);
		boolean result = run(context, qcStats);

		assertTrue(result);
		assertTrue(context.hasInMemory("Remaining sites in total: 7,735"));

		// add panel to hdfs
		importRefPanel(FileUtil.path(configFolder, "ref-panels"));
		// importMinimacMap("test-data/B38_MAP_FILE.map");
		importBinaries("files/bin");

		// run imputation
		ImputationMinimac3Mock imputation = new ImputationMinimac3Mock(configFolder);
		result = run(context, imputation);
		assertTrue(result);

		LineReader reader = new LineReader("test-data/tmp/statisticDir/typed-only.txt");
		int count = 0;

		while (reader.next()) {
			count++;
		}
		reader.close();

		assertEquals(count, ONLY_IN_INPUT + 1);

		FileUtil.deleteDirectory("test-data/tmp");

	}

	@Test
	public void testPipelineWithPhasedHg19ToHg38() throws IOException, ZipException {

		String configFolder = "test-data/configs/hapmap-chr20-hg38";
		String inputFolder = "test-data/data/chr20-phased";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, "hapmap2");

		// run qc to create chunkfile
		QcStatisticsMock qcStats = new QcStatisticsMock(configFolder);
		boolean result = run(context, qcStats);

		assertTrue(result);
		assertTrue(context.hasInMemory("Remaining sites in total: 7,735"));

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

		ZipFile zipFile = new ZipFile("test-data/tmp/local/chr_20.zip", PASSWORD.toCharArray());
		zipFile.extractAll("test-data/tmp");

		VcfFile file = VcfFileUtil.load("test-data/tmp/chr20.dose.vcf.gz", 100000000, false);

		assertEquals("20", file.getChromosome());
		assertEquals(51, file.getNoSamples());
		assertEquals(true, file.isPhased());
		assertEquals(TOTAL_REFPANEL_CHR20_B38 + ONLY_IN_INPUT, file.getNoSnps());

		FileUtil.deleteDirectory("test-data/tmp");

	}

	@Test
	public void testPipelineWithEagleHg19ToHg38() throws IOException, ZipException {

		String configFolder = "test-data/configs/hapmap-chr20-hg38";
		String inputFolder = "test-data/data/chr20-unphased";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, "hapmap2");

		// run qc to create chunkfile
		QcStatisticsMock qcStats = new QcStatisticsMock(configFolder);
		boolean result = run(context, qcStats);

		assertTrue(result);
		assertTrue(context.hasInMemory("Remaining sites in total: 7,735"));

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

		ZipFile zipFile = new ZipFile("test-data/tmp/local/chr_20.zip", PASSWORD.toCharArray());
		zipFile.extractAll("test-data/tmp");

		VcfFile file = VcfFileUtil.load("test-data/tmp/chr20.dose.vcf.gz", 100000000, false);

		assertEquals("20", file.getChromosome());
		assertEquals(51, file.getNoSamples());
		assertEquals(true, file.isPhased());

		assertEquals(TOTAL_REFPANEL_CHR20_B38, file.getNoSnps());

		FileUtil.deleteDirectory("test-data/tmp");

	}

	@Test
	public void testPipelineWithEagleHg38ToHg38() throws IOException, ZipException {

		String configFolder = "test-data/configs/hapmap-chr20-hg38";
		String inputFolder = "test-data/data/chr20-unphased-hg38";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, "hapmap2");
		context.setInput("build", "hg38");

		// run qc to create chunkfile
		QcStatisticsMock qcStats = new QcStatisticsMock(configFolder);
		boolean result = run(context, qcStats);

		assertTrue(result);
		assertTrue(context.hasInMemory("Remaining sites in total: 7,735"));

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

		ZipFile zipFile = new ZipFile("test-data/tmp/local/chr_20.zip", PASSWORD.toCharArray());
		zipFile.extractAll("test-data/tmp");

		VcfFile file = VcfFileUtil.load("test-data/tmp/chr20.dose.vcf.gz", 100000000, false);

		assertEquals("20", file.getChromosome());
		assertEquals(51, file.getNoSamples());
		assertEquals(true, file.isPhased());

		assertEquals(TOTAL_REFPANEL_CHR20_B38, file.getNoSnps());

		FileUtil.deleteDirectory("test-data/tmp");

	}

	@Test
	public void testPipelineWithPhasedHg38ToHg19() throws IOException, ZipException {

		String configFolder = "test-data/configs/hapmap-chr20";
		String inputFolder = "test-data/data/chr20-phased-hg38";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, "hapmap2");
		context.setInput("build", "hg38");

		// run qc to create chunkfile
		QcStatisticsMock qcStats = new QcStatisticsMock(configFolder);
		boolean result = run(context, qcStats);

		assertTrue(result);
		assertTrue(context.hasInMemory("Remaining sites in total: 7,735"));

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

		ZipFile zipFile = new ZipFile("test-data/tmp/local/chr_20.zip", PASSWORD.toCharArray());
		zipFile.extractAll("test-data/tmp");

		VcfFile file = VcfFileUtil.load("test-data/tmp/chr20.dose.vcf.gz", 100000000, false);

		assertEquals("20", file.getChromosome());
		assertEquals(51, file.getNoSamples());
		assertEquals(true, file.isPhased());
		assertEquals(TOTAL_REFPANEL_CHR20_B37 + ONLY_IN_INPUT, file.getNoSnps());

		FileUtil.deleteDirectory("test-data/tmp");

	}

	@Test
	public void testPipelineWithEagleHg38ToHg19() throws IOException, ZipException {

		String configFolder = "test-data/configs/hapmap-chr20";
		String inputFolder = "test-data/data/chr20-unphased-hg38";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, "hapmap2");
		context.setInput("build", "hg38");

		// run qc to create chunkfile
		QcStatisticsMock qcStats = new QcStatisticsMock(configFolder);
		boolean result = run(context, qcStats);

		assertTrue(result);
		assertTrue(context.hasInMemory("Remaining sites in total: 7,735"));

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

		ZipFile zipFile = new ZipFile("test-data/tmp/local/chr_20.zip", PASSWORD.toCharArray());
		zipFile.extractAll("test-data/tmp");

		VcfFile file = VcfFileUtil.load("test-data/tmp/chr20.dose.vcf.gz", 100000000, false);

		assertEquals("20", file.getChromosome());
		assertEquals(51, file.getNoSamples());
		assertEquals(true, file.isPhased());

		assertEquals(TOTAL_REFPANEL_CHR20_B37, file.getNoSnps());

		FileUtil.deleteDirectory("test-data/tmp");

	}

	protected boolean run(WorkflowTestContext context, WorkflowStep step) {
		step.setup(context);
		return step.run(context);
	}

	protected WorkflowTestContext buildContext(String folder, String refpanel) {
		WorkflowTestContext context = new WorkflowTestContext();
		File file = new File("test-data/tmp");
		if (file.exists()) {
			FileUtil.deleteDirectory(file);
		}
		file.mkdirs();

		HdfsUtil.delete("cloudgene-hdfs");

		context.setVerbose(VERBOSE);
		context.setInput("files", folder);
		context.setInput("population", "eur");
		context.setInput("refpanel", refpanel);
		context.setInput("mode", "imputation");
		context.setInput("password", PASSWORD);
		context.setConfig("binaries", BINARIES_HDFS);

		context.setOutput("mafFile", file.getAbsolutePath() + "/mafFile/mafFile.txt");
		FileUtil.createDirectory(file.getAbsolutePath() + "/mafFile");

		context.setOutput("chunkFileDir", file.getAbsolutePath() + "/chunkFileDir");
		FileUtil.createDirectory(file.getAbsolutePath() + "/chunkFileDir");

		context.setOutput("statisticDir", file.getAbsolutePath() + "/statisticDir");
		FileUtil.createDirectory(file.getAbsolutePath() + "/statisticDir");

		context.setOutput("chunksDir", file.getAbsolutePath() + "/chunksDir");
		FileUtil.createDirectory(file.getAbsolutePath() + "/chunksDir");

		context.setOutput("local", file.getAbsolutePath() + "/local");
		FileUtil.createDirectory(file.getAbsolutePath() + "/local");

		context.setOutput("logfile", file.getAbsolutePath() + "/logfile");
		FileUtil.createDirectory(file.getAbsolutePath() + "/logfile");

		context.setHdfsTemp("minimac-temp");
		HdfsUtil.createDirectory(context.getHdfsTemp());

		context.setOutput("outputimputation", "cloudgene-hdfs");

		context.setOutput("hadooplogs", file.getAbsolutePath() + "/hadooplogs");
		FileUtil.deleteDirectory(file.getAbsolutePath() + "/hadooplogs");
		FileUtil.createDirectory(file.getAbsolutePath() + "/hadooplogs");

		context.setLocalTemp("local-temp");
		FileUtil.deleteDirectory("local-temp");
		FileUtil.createDirectory("local-temp");

		return context;

	}

	private void importMinimacMap2(String file) {
		System.out.println("Import Minimac Map");
		String target = HdfsUtil.path("meta", FileUtil.getFilename(file));
		System.out.println("  Import " + file + " to " + target);
		HdfsUtil.put(file, target);
	}

	private void importRefPanel(String folder) {
		System.out.println("Import Reference Panels:");
		String[] files = FileUtil.getFiles(folder, "*.*");
		for (String file : files) {
			String target = HdfsUtil.path("ref-panels", FileUtil.getFilename(file));
			System.out.println("  Import " + file + " to " + target);
			HdfsUtil.put(file, target);
		}
	}

	private void importBinaries(String folder) {
		System.out.println("Import Binaries to " + BINARIES_HDFS);
		String[] files = FileUtil.getFiles(folder, "*.*");
		for (String file : files) {
			String target = HdfsUtil.path(BINARIES_HDFS, FileUtil.getFilename(file));
			System.out.println("  Import " + file + " to " + target);
			HdfsUtil.put(file, target);
		}
	}

	class CompressionEncryptionMock extends CompressionEncryption {

		private String folder;

		public CompressionEncryptionMock(String folder) {
			super();
			this.folder = folder;
		}

		@Override
		public String getFolder(Class clazz) {
			// override folder with static folder instead of jar location
			return folder;
		}

	}

	class ImputationMinimac3Mock extends Imputation {

		private String folder;

		public ImputationMinimac3Mock(String folder) {
			super();
			this.folder = folder;
		}

		@Override
		public String getFolder(Class clazz) {
			// override folder with static folder instead of jar location
			return folder;
		}

	}

	class QcStatisticsMock extends FastQualityControl {

		private String folder;

		public QcStatisticsMock(String folder) {
			super();
			this.folder = folder;
		}

		@Override
		public String getFolder(Class clazz) {
			// override folder with static folder instead of jar location
			return folder;
		}

		@Override
		protected void setupTabix(String folder) {
			VcfFileUtil.setTabixBinary("files/bin/tabix");
		}

	}

	class InputValidationMock extends InputValidation {

		private String folder;

		public InputValidationMock(String folder) {
			super();
			this.folder = folder;
		}

		@Override
		public String getFolder(Class clazz) {
			// override folder with static folder instead of jar location
			return folder;
		}

		@Override
		protected void setupTabix(String folder) {
			VcfFileUtil.setTabixBinary("files/bin/tabix");
		}

	}
}
