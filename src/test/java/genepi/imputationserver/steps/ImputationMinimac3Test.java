package genepi.imputationserver.steps;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import genepi.hadoop.HdfsUtil;
import genepi.hadoop.common.WorkflowStep;
import genepi.imputationserver.steps.InputValidationTest.InputValidationMock;
import genepi.imputationserver.steps.imputationMinimac3.ImputationJobMinimac3;
import genepi.imputationserver.steps.vcf.VcfFile;
import genepi.imputationserver.steps.vcf.VcfFileUtil;
import genepi.imputationserver.util.TestCluster;
import genepi.imputationserver.util.TestSFTPServer;
import genepi.imputationserver.util.WorkflowTestContext;
import genepi.io.FileUtil;
import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;

public class ImputationMinimac3Test {

	public static final boolean VERBOSE = true;

	public final int TOTAL_REFPANEL_CHR20 = 63407;
	public final int TOTAL_REFPANEL_CHRX_NONPAR = 1437122;
	public final int FILTER_REFPANEL = 5;
	public final int ONLY_IN_INPUT = 78;

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
		WorkflowTestContext context = buildContext(inputFolder, "hapmap2", "eagle");

		// run qc to create chunkfile
		QcStatisticsMock qcStats = new QcStatisticsMock(configFolder);
		boolean result = run(context, qcStats);

		assertTrue(result);
		assertTrue(context.hasInMemory("Remaining sites in total: 7,735"));

		// add panel to hdfs
		importRefPanel(FileUtil.path(configFolder, "ref-panels"));
		importBinaries("files/minimac/bin");

		// run imputation
		ImputationMinimac3Mock imputation = new ImputationMinimac3Mock(configFolder);
		result = run(context, imputation);
		assertTrue(result);

		// run export
		CompressionEncryptionMock export = new CompressionEncryptionMock("files/minimac");
		result = run(context, export);
		assertTrue(result);

		ZipFile zipFile = new ZipFile("test-data/tmp/local/chr_20.zip");
		if (zipFile.isEncrypted()) {
			zipFile.setPassword(CompressionEncryption.DEFAULT_PASSWORD);
		}
		zipFile.extractAll("test-data/tmp");

		VcfFile file = VcfFileUtil.load("test-data/tmp/chr20.dose.vcf.gz", 100000000, false);

		assertEquals("20", file.getChromosome());
		assertEquals(51, file.getNoSamples());
		assertEquals(true, file.isPhased());
		assertEquals(TOTAL_REFPANEL_CHR20 - FILTER_REFPANEL + ONLY_IN_INPUT, file.getNoSnps());

		FileUtil.deleteDirectory("test-data/tmp");

	}

	@Test
	public void testPipelineWidthInvalidHttpUrl() throws IOException, ZipException {

		String configFolder = "test-data/configs/hapmap-chr1";
		String inputFolder = "https://imputationserver.sph.umich.edu/invalid-url/downloads/hapmap300.chr1.recode.vcf.gz";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, "hapmap2", "shapeit");

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
		WorkflowTestContext context = buildContext(inputFolder, "hapmap2", "shapeit");

		// create step instance
		InputValidation inputValidation = new InputValidationMock(configFolder);

		// run and test
		boolean result = run(context, inputValidation);

		// check if step is failed
		assertEquals(false, result);

		FileUtil.deleteDirectory("test-data/tmp");

	}

	@Test
	public void testPipelineWidthHttpUrl() throws IOException, ZipException {

		String configFolder = "test-data/configs/hapmap-chr1";
		String inputFolder = "https://imputationserver.sph.umich.edu/static/downloads/hapmap300.chr1.recode.vcf.gz";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, "hapmap2", "shapeit");

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
		importBinaries("files/minimac/bin");

		// run imputation
		ImputationMinimac3Mock imputation = new ImputationMinimac3Mock(configFolder);
		result = run(context, imputation);
		assertTrue(result);

		// run export
		CompressionEncryptionMock export = new CompressionEncryptionMock("files/minimac");
		result = run(context, export);
		assertTrue(result);

		ZipFile zipFile = new ZipFile("test-data/tmp/local/chr_1.zip");
		if (zipFile.isEncrypted()) {
			zipFile.setPassword(CompressionEncryption.DEFAULT_PASSWORD);
		}
		zipFile.extractAll("test-data/tmp");

		VcfFile file = VcfFileUtil.load("test-data/tmp/chr1.dose.vcf.gz", 100000000, false);

		assertEquals("1", file.getChromosome());
		assertEquals(60, file.getNoSamples());
		assertEquals(true, file.isPhased());

		FileUtil.deleteDirectory("test-data/tmp");

	}

	@Test
	public void testPipelineWithSFTP() throws IOException, ZipException, InterruptedException {

		TestSFTPServer server = new TestSFTPServer("test-data/data");

		String configFolder = "test-data/configs/hapmap-chr20";
		String inputFolder = "sftp://localhost:8001/" + new File("test-data/data/chr20-phased").getAbsolutePath() + ";"
				+ TestSFTPServer.USERNAME + ";" + TestSFTPServer.PASSWORD;

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, "hapmap2", "eagle");

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
		importBinaries("files/minimac/bin");

		// run imputation
		ImputationMinimac3Mock imputation = new ImputationMinimac3Mock(configFolder);
		result = run(context, imputation);
		assertTrue(result);

		// run export
		CompressionEncryptionMock export = new CompressionEncryptionMock("files/minimac");
		result = run(context, export);
		assertTrue(result);

		ZipFile zipFile = new ZipFile("test-data/tmp/local/chr_20.zip");
		if (zipFile.isEncrypted()) {
			zipFile.setPassword(CompressionEncryption.DEFAULT_PASSWORD);
		}
		zipFile.extractAll("test-data/tmp");

		VcfFile file = VcfFileUtil.load("test-data/tmp/chr20.dose.vcf.gz", 100000000, false);

		assertEquals("20", file.getChromosome());
		assertEquals(51, file.getNoSamples());
		assertEquals(true, file.isPhased());
		assertEquals(TOTAL_REFPANEL_CHR20 - FILTER_REFPANEL + ONLY_IN_INPUT, file.getNoSnps());

		FileUtil.deleteDirectory("test-data/tmp");

		server.stop();

	}

	@Test
	public void testPipelineWithWrongSFTPCredentials() throws IOException, ZipException, InterruptedException {

		TestSFTPServer server = new TestSFTPServer("test-data/data");

		String configFolder = "test-data/configs/hapmap-chr20";
		String inputFolder = "sftp://localhost:8001/" + new File("data/chr20-phased").getAbsolutePath() + ";"
				+ "WRONG_USERNAME" + ";" + TestSFTPServer.PASSWORD;

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, "hapmap2", "eagle");

		// create step instance
		InputValidation inputValidation = new InputValidationMock(configFolder);

		// run and test
		boolean result = run(context, inputValidation);

		// check if step is failed
		assertEquals(false, result);

		server.stop();

	}

	@Test
	public void testPipelineWithEagle() throws IOException, ZipException {

		String configFolder = "test-data/configs/hapmap-chr20";
		String inputFolder = "test-data/data/chr20-unphased";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, "hapmap2", "eagle");

		// run qc to create chunkfile
		QcStatisticsMock qcStats = new QcStatisticsMock(configFolder);
		boolean result = run(context, qcStats);

		assertTrue(result);
		assertTrue(context.hasInMemory("Remaining sites in total: 7,735"));

		// add panel to hdfs
		importRefPanel(FileUtil.path(configFolder, "ref-panels"));
		importBinaries("files/minimac/bin");

		// run imputation
		ImputationMinimac3Mock imputation = new ImputationMinimac3Mock(configFolder);
		result = run(context, imputation);
		assertTrue(result);

		// run export
		CompressionEncryptionMock export = new CompressionEncryptionMock("files/minimac");
		result = run(context, export);
		assertTrue(result);

		ZipFile zipFile = new ZipFile("test-data/tmp/local/chr_20.zip");
		if (zipFile.isEncrypted()) {
			zipFile.setPassword(CompressionEncryption.DEFAULT_PASSWORD);
		}
		zipFile.extractAll("test-data/tmp");

		VcfFile file = VcfFileUtil.load("test-data/tmp/chr20.dose.vcf.gz", 100000000, false);

		assertEquals("20", file.getChromosome());
		assertEquals(51, file.getNoSamples());
		assertEquals(true, file.isPhased());
		assertEquals(TOTAL_REFPANEL_CHR20 - FILTER_REFPANEL  + ONLY_IN_INPUT, file.getNoSnps());

		FileUtil.deleteDirectory("test-data/tmp");

	}

	@Test
	public void testPipelineWithEagleAnd23AndMe() throws IOException, ZipException {

		String configFolder = "test-data/configs/hapmap-chr20";
		String inputFolder = "test-data/data/23andMe";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, "hapmap2", "eagle");

		// create step instance
		InputValidation inputValidation = new InputValidationMock(configFolder);

		// run and test
		boolean result = run(context, inputValidation);

		// check if step is failed
		assertEquals(true, result);

		FileUtil.deleteDirectory("test-data/tmp");

	}

	
	@Test
	public void testPipelineWithShapeIt() throws IOException, ZipException {

		if (!new File("files/minimac/bin/shapeit").exists()) {
			return;
		}

		String configFolder = "test-data/configs/hapmap-chr20";
		String inputFolder = "test-data/data/chr20-unphased";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, "hapmap2", "shapeit");

		// run qc to create chunkfile
		QcStatisticsMock qcStats = new QcStatisticsMock(configFolder);
		boolean result = run(context, qcStats);

		assertTrue(result);
		assertTrue(context.hasInMemory("Remaining sites in total: 7,735"));

		// add panel to hdfs
		importRefPanel(FileUtil.path(configFolder, "ref-panels"));
		importBinaries("files/minimac/bin");

		// run imputation
		ImputationMinimac3Mock imputation = new ImputationMinimac3Mock(configFolder);
		result = run(context, imputation);
		assertTrue(result);

		// run export
		CompressionEncryptionMock export = new CompressionEncryptionMock("files/minimac");
		result = run(context, export);
		assertTrue(result);

		ZipFile zipFile = new ZipFile("test-data/tmp/local/chr_20.zip");
		if (zipFile.isEncrypted()) {
			zipFile.setPassword(CompressionEncryption.DEFAULT_PASSWORD);
		}
		zipFile.extractAll("test-data/tmp");

		VcfFile file = VcfFileUtil.load("test-data/tmp/chr20.dose.vcf.gz", 100000000, false);

		assertEquals("20", file.getChromosome());
		assertEquals(51, file.getNoSamples());
		assertEquals(false, file.isPhased());
		assertEquals(TOTAL_REFPANEL_CHR20 - FILTER_REFPANEL + ONLY_IN_INPUT, file.getNoSnps());

		FileUtil.deleteDirectory("test-data/tmp");

	}

	@Test
	public void testPipelineWithHapiUr() throws IOException, ZipException {

		if (!new File("files/minimac/bin/hapi-ur").exists()) {
			return;
		}

		String configFolder = "test-data/configs/hapmap-chr20";
		String inputFolder = "test-data/data/chr20-unphased";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, "hapmap2", "hapiur");

		// run qc to create chunkfile
		QcStatisticsMock qcStats = new QcStatisticsMock(configFolder);
		boolean result = run(context, qcStats);

		assertTrue(result);
		assertTrue(context.hasInMemory("Remaining sites in total: 7,735"));

		// add panel to hdfs
		importRefPanel(FileUtil.path(configFolder, "ref-panels"));
		importBinaries("files/minimac/bin");

		// run imputation
		ImputationMinimac3Mock imputation = new ImputationMinimac3Mock(configFolder);
		result = run(context, imputation);
		assertTrue(result);

		// run export
		CompressionEncryptionMock export = new CompressionEncryptionMock("files/minimac");
		result = run(context, export);
		assertTrue(result);

		ZipFile zipFile = new ZipFile("test-data/tmp/local/chr_20.zip");
		if (zipFile.isEncrypted()) {
			zipFile.setPassword(CompressionEncryption.DEFAULT_PASSWORD);
		}
		zipFile.extractAll("test-data/tmp");

		VcfFile file = VcfFileUtil.load("test-data/tmp/chr20.dose.vcf.gz", 100000000, false);

		assertEquals("20", file.getChromosome());
		assertEquals(51, file.getNoSamples());
		assertEquals(true, file.isPhased());
		assertEquals(TOTAL_REFPANEL_CHR20 - FILTER_REFPANEL + ONLY_IN_INPUT, file.getNoSnps());

		FileUtil.deleteDirectory("test-data/tmp");

	}

	@Test
	public void testchrXPipelineWithEagle() throws IOException, ZipException {

		// maybe git large files?
		if (!new File(
				"test-data/configs/hapmap-chrX/ref-panels/ALL.chrX.Non.Pseudo.Auto.phase1_v3.snps_indels_svs.genotypes.all.noSingleton.recode.bcf")
						.exists()) {
			return;
		}

		String configFolder = "test-data/configs/hapmap-chrX";
		String inputFolder = "test-data/data/chrX-unphased";

		File file = new File("test-data/tmp");
		if (file.exists()) {
			FileUtil.deleteDirectory(file);
		}

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, "phase1", "eagle");

		// run qc to create chunkfile
		QcStatisticsMock qcStats = new QcStatisticsMock(configFolder);
		boolean result = run(context, qcStats);

		assertTrue(result);

		// add panel to hdfs
		importRefPanel(FileUtil.path(configFolder, "ref-panels"));
		importBinaries("files/minimac/bin");

		// run imputation
		ImputationMinimac3Mock imputation = new ImputationMinimac3Mock(configFolder);
		result = run(context, imputation);
		assertTrue(result);

		// run export
		CompressionEncryptionMock export = new CompressionEncryptionMock("files/minimac");
		result = run(context, export);
		assertTrue(result);

		ZipFile zipFile = new ZipFile("test-data/tmp/local/chr_X.Non.Pseudo.Auto.zip");
		if (zipFile.isEncrypted()) {
			zipFile.setPassword(CompressionEncryption.DEFAULT_PASSWORD);
		}
		zipFile.extractAll("test-data/tmp");

		VcfFile vcfFile = VcfFileUtil.load("test-data/tmp/chrX.Non.Pseudo.Auto.dose.vcf.gz", 100000000, false);

		assertEquals("X", vcfFile.getChromosome());

		FileUtil.deleteDirectory(file);

	}

	@Test
	public void testchrXPipelinePhased() throws IOException, ZipException {

		String configFolder = "test-data/configs/hapmap-chrX";
		String inputFolder = "test-data/data/chrX-phased";

		File file = new File("test-data/tmp");
		if (file.exists()) {
			FileUtil.deleteDirectory(file);
		}

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, "phase1", "eagle-phasing");

		// run qc to create chunkfile
		QcStatisticsMock qcStats = new QcStatisticsMock(configFolder);
		boolean result = run(context, qcStats);

		assertTrue(result);

		// add panel to hdfs
		importRefPanel(FileUtil.path(configFolder, "ref-panels"));
		importBinaries("files/minimac/bin");

		// run imputation
		ImputationMinimac3Mock imputation = new ImputationMinimac3Mock(configFolder);
		result = run(context, imputation);
		assertTrue(result);

		// run export
		CompressionEncryptionMock export = new CompressionEncryptionMock("files/minimac");
		result = run(context, export);
		assertTrue(result);

		ZipFile zipFile = new ZipFile("test-data/tmp/local/chr_X.Non.Pseudo.Auto.zip");
		if (zipFile.isEncrypted()) {
			zipFile.setPassword(CompressionEncryption.DEFAULT_PASSWORD);
		}
		zipFile.extractAll("test-data/tmp");

		VcfFile vcfFile = VcfFileUtil.load("test-data/tmp/chrX.Non.Pseudo.Auto.dose.vcf.gz", 100000000, false);

		assertEquals("X", vcfFile.getChromosome());
		assertEquals(26, vcfFile.getNoSamples());
		assertEquals(true, vcfFile.isPhased());
		assertEquals(TOTAL_REFPANEL_CHRX_NONPAR, vcfFile.getNoSnps());

		// FileUtil.deleteDirectory(file);

	}

	protected boolean run(WorkflowTestContext context, WorkflowStep step) {
		step.setup(context);
		return step.run(context);
	}

	protected WorkflowTestContext buildContext(String folder, String refpanel, String phasing) {
		WorkflowTestContext context = new WorkflowTestContext();
		File file = new File("test-data/tmp");
		if (file.exists()) {
			FileUtil.deleteDirectory(file);
		}
		file.mkdirs();
		context.setVerbose(VERBOSE);
		context.setInput("files", folder);
		context.setInput("population", "eur");
		context.setInput("refpanel", refpanel);
		context.setInput("chunksize", "10000000");
		context.setInput("phasingsize", "5000000");
		context.setInput("rounds", "0");
		context.setInput("window", "500000");
		context.setInput("phasing", phasing);
		context.setInput("sample-limit", "0");
		context.setInput("minimacbin", "Minimac4");

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
		System.out.println("Import Binaries:");
		String[] files = FileUtil.getFiles(folder, "*.*");
		for (String file : files) {
			String target = HdfsUtil.path(ImputationJobMinimac3.DATA_FOLDER, FileUtil.getFilename(file));
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

	class ImputationMinimac3Mock extends ImputationMinimac3 {

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

	class QcStatisticsMock extends QualityControl {

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
			VcfFileUtil.setBinary("files/minimac/bin/tabix");
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
			VcfFileUtil.setBinary("files/minimac/bin/tabix");
		}

	}
}
