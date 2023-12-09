package genepi.imputationserver.steps;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import cloudgene.sdk.internal.WorkflowStep;
import genepi.hadoop.HdfsUtil;
import genepi.imputationserver.steps.imputation.ImputationPipeline;
import genepi.imputationserver.steps.vcf.VcfFile;
import genepi.imputationserver.steps.vcf.VcfFileUtil;
import genepi.imputationserver.util.TestCluster;
import genepi.imputationserver.util.WorkflowTestContext;
import genepi.io.FileUtil;
import genepi.io.table.reader.CsvTableReader;
import genepi.io.text.LineReader;
import genepi.riskscore.commands.ApplyScoreCommand;
import genepi.riskscore.io.PGSCatalog;
import htsjdk.variant.vcf.VCFFileReader;
import htsjdk.variant.vcf.VCFHeader;
import net.lingala.zip4j.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import picocli.CommandLine;

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

		FileUtil.deleteDirectory("test-data/tmp");

	}

	@Test
	public void testPipelineWithPhasedAndMetaOption() throws IOException, ZipException {

		String configFolder = "test-data/configs/hapmap-chr20";
		String inputFolder = "test-data/data/chr20-phased";

		// create workflow context

		WorkflowTestContext context = buildContext(inputFolder, "hapmap2");
		context.setInput("meta", "yes");

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

		VcfFile fileMeta = VcfFileUtil.load("test-data/tmp/chr20.empiricalDose.vcf.gz", 100000000, false);

		// count GENOTYPED from info file
		assertEquals(7735, fileMeta.getNoSnps());
		assertEquals("20", file.getChromosome());
		assertEquals(51, file.getNoSamples());
		assertEquals(true, file.isPhased());
		assertEquals(TOTAL_REFPANEL_CHR20_B37 + ONLY_IN_INPUT, file.getNoSnps());

		FileUtil.deleteDirectory("test-data/tmp");

	}

	@Test
	public void testPipelineWithPhasedAndEmptyPhasing() throws IOException, ZipException {

		String configFolder = "test-data/configs/hapmap-chr20";
		String inputFolder = "test-data/data/chr20-phased";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, "hapmap2");
		context.setInput("phasing", "");

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

		int snpInInfo = getLineCount("test-data/tmp/chr20.info.gz");
		assertEquals(snpInInfo, file.getNoSnps());

		FileUtil.deleteDirectory("test-data/tmp");

	}

	@Test
	public void testValidatePanelWithEagle() throws IOException, ZipException {

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

		VCFFileReader reader = new VCFFileReader(new File("test-data/tmp/chr20.dose.vcf.gz"), false);
		VCFHeader header = reader.getFileHeader();
		assertEquals("hapmap2", header.getOtherHeaderLine("mis_panel").getValue());
		assertEquals(ImputationPipeline.EAGLE_VERSION, header.getOtherHeaderLine("mis_phasing").getValue());
		assertEquals(ImputationPipeline.IMPUTATION_VERSION, header.getOtherHeaderLine("mis_imputation").getValue());
		assertEquals(ImputationPipeline.PIPELINE_VERSION, header.getOtherHeaderLine("mis_pipeline").getValue());

		FileUtil.deleteDirectory("test-data/tmp");

	}

	@Test
	public void testValidatePanelWithBeagle() throws IOException, ZipException {

		String configFolder = "test-data/configs/beagle";
		String inputFolder = "test-data/data/chr20-unphased";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, "hapmap2");

		context.setInput("phasing", "beagle");

		// run qc to create chunkfile
		QcStatisticsMock qcStats = new QcStatisticsMock(configFolder);
		boolean result = run(context, qcStats);

		assertTrue(result);
		assertTrue(context.hasInMemory("Remaining sites in total: 7,735"));

		// add panel to hdfs
		importRefPanel(FileUtil.path(configFolder, "ref-panels"));
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

		VCFFileReader reader = new VCFFileReader(new File("test-data/tmp/chr20.dose.vcf.gz"), false);
		VCFHeader header = reader.getFileHeader();
		assertEquals("hapmap2", header.getOtherHeaderLine("mis_panel").getValue());
		assertEquals(ImputationPipeline.BEAGLE_VERSION, header.getOtherHeaderLine("mis_phasing").getValue());
		assertEquals(ImputationPipeline.IMPUTATION_VERSION, header.getOtherHeaderLine("mis_imputation").getValue());
		assertEquals(ImputationPipeline.PIPELINE_VERSION, header.getOtherHeaderLine("mis_pipeline").getValue());

		FileUtil.deleteDirectory("test-data/tmp");

	}

	@Test
	public void testValidatePanelPhasingOnly() throws IOException, ZipException {

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

		VCFFileReader reader = new VCFFileReader(new File("test-data/tmp/chr20.phased.vcf.gz"), false);
		VCFHeader header = reader.getFileHeader();
		assertEquals("hapmap2", header.getOtherHeaderLine("mis_panel").getValue());
		assertEquals(ImputationPipeline.EAGLE_VERSION, header.getOtherHeaderLine("mis_phasing").getValue());
		assertEquals(ImputationPipeline.PIPELINE_VERSION, header.getOtherHeaderLine("mis_pipeline").getValue());

		FileUtil.deleteDirectory("test-data/tmp");

	}

	@Test
	public void testValidatePanelPhasedInput() throws IOException, ZipException {

		String configFolder = "test-data/configs/hapmap-chr20";
		String inputFolder = "test-data/data/chr20-phased";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, "hapmap2");

		context.setInput("phasing", "no_phasing");

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

		VCFFileReader reader = new VCFFileReader(new File("test-data/tmp/chr20.dose.vcf.gz"), false);
		VCFHeader header = reader.getFileHeader();
		assertEquals("hapmap2", header.getOtherHeaderLine("mis_panel").getValue());
		assertEquals("n/a", header.getOtherHeaderLine("mis_phasing").getValue());
		assertEquals(ImputationPipeline.PIPELINE_VERSION, header.getOtherHeaderLine("mis_pipeline").getValue());

		FileUtil.deleteDirectory("test-data/tmp");

	}

	@Test
	public void testPipelineWithInvalidPgsBuild() throws IOException, ZipException {

		String configFolder = "test-data/configs/hapmap-chr20";
		String inputFolder = "test-data/data/chr20-unphased";

		// import scores into hdfs
		String score1 = PGSCatalog.getFilenameById("PGS000018");
		String score2 = PGSCatalog.getFilenameById("PGS000027");

		String targetScore1 = HdfsUtil.path("scores-hdfs", "PGS000018.txt.gz");
		HdfsUtil.put(score1, targetScore1);

		String targetScore2 = HdfsUtil.path("scores-hdfs", "PGS000027.txt.gz");
		HdfsUtil.put(score2, targetScore2);

		// create workflow context and set scores
		WorkflowTestContext context = buildContext(inputFolder, "hapmap2");
		context.setOutput("outputScores", "cloudgene2-hdfs");

		Map<String, Object> pgsPanel = new HashMap<String, Object>();
		List<String> scores = new Vector<String>();
		scores.add("PGS000018.txt.gz");
		scores.add("PGS000027.txt.gz");
		pgsPanel.put("location", "scores-hdfs");
		pgsPanel.put("scores", scores);
		pgsPanel.put("build", "hg38");
		context.setData("pgsPanel", pgsPanel);

		// run qc to create chunkfile

		InputValidation inputValidation = new InputValidationMock(configFolder);
		// run and test
		boolean result = run(context, inputValidation);
		// check if step is failed
		assertEquals(false, result);

	}

	@Test
	public void testPipelineWithEagleAndScores() throws IOException, ZipException {

		String configFolder = "test-data/configs/hapmap-chr20";
		String inputFolder = "test-data/data/chr20-unphased";

		// import scores into hdfs
		String targetScores = HdfsUtil.path("scores-hdfs", "scores.txt.gz");
		HdfsUtil.put("test-data/data/pgs/test-scores.chr20.txt.gz", targetScores);

		String targetIndex = HdfsUtil.path("scores-hdfs", "scores.txt.gz.tbi");
		HdfsUtil.put("test-data/data/pgs/test-scores.chr20.txt.gz.tbi", targetIndex);

		String targetInfo = HdfsUtil.path("scores-hdfs", "scores.txt.gz.info");
		HdfsUtil.put("test-data/data/pgs/test-scores.chr20.txt.gz.info", targetInfo);

		String targetJson = HdfsUtil.path("scores-hdfs", "scores.json");
		HdfsUtil.put("test-data/data/pgs/test-scores.chr20.json", targetJson);

		// create workflow context and set scores
		WorkflowTestContext context = buildContext(inputFolder, "hapmap2");
		context.setOutput("outputScores", "cloudgene2-hdfs");

		Map<String, Object> pgsPanel = new HashMap<String, Object>();
		pgsPanel.put("scores", targetScores);
		pgsPanel.put("meta", targetJson);
		pgsPanel.put("build", "hg19");
		context.setData("pgsPanel", pgsPanel);

		// run qc to create chunkfile

		InputValidation inputValidation = new InputValidationMock(configFolder);
		// run and test
		boolean result = run(context, inputValidation);
		assertTrue(result);

		QcStatisticsMock qcStats = new QcStatisticsMock(configFolder);
		result = run(context, qcStats);

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

		ZipFile zipFile = new ZipFile("test-data/tmp/pgs_output/scores.zip", PASSWORD.toCharArray());
		zipFile.extractAll("test-data/tmp");
		CsvTableReader readerExpected = new CsvTableReader("test-data/data/pgs/expected.txt", ',');
		CsvTableReader readerActual = new CsvTableReader("test-data/tmp/scores.txt", ',');

		while (readerExpected.next() && readerActual.next()) {
			assertEquals(readerExpected.getDouble("PGS000018"), readerActual.getDouble("PGS000018"), 0.00001);
			assertEquals(readerExpected.getDouble("PGS000027"), readerActual.getDouble("PGS000027"), 0.00001);
		}
		readerExpected.close();
		readerActual.close();

		// check if html report file exisits
		new File("test-data/tmp/local/scores.html").exists();

		FileUtil.deleteDirectory("test-data/tmp");
		zipFile.close();

	}

	@Test
	public void testPipelineWithEagleAndScoresAndCategory() throws IOException, ZipException {

		String configFolder = "test-data/configs/hapmap-chr20";
		String inputFolder = "test-data/data/chr20-unphased";

		// import scores into hdfs
		String targetScores = HdfsUtil.path("scores-hdfs", "scores.txt.gz");
		HdfsUtil.put("test-data/data/pgs/test-scores.chr20.txt.gz", targetScores);

		String targetIndex = HdfsUtil.path("scores-hdfs", "scores.txt.gz.tbi");
		HdfsUtil.put("test-data/data/pgs/test-scores.chr20.txt.gz.tbi", targetIndex);

		String targetInfo = HdfsUtil.path("scores-hdfs", "scores.txt.gz.info");
		HdfsUtil.put("test-data/data/pgs/test-scores.chr20.txt.gz.info", targetInfo);

		String targetJson = HdfsUtil.path("scores-hdfs", "scores.json");
		HdfsUtil.put("test-data/data/pgs/test-scores.chr20.json", targetJson);

		// create workflow context and set scores
		WorkflowTestContext context = buildContext(inputFolder, "hapmap2");
		context.setOutput("outputScores", "cloudgene2-hdfs");

		Map<String, Object> pgsPanel = new HashMap<String, Object>();
		pgsPanel.put("scores", targetScores);
		pgsPanel.put("meta", targetJson);
		pgsPanel.put("build", "hg19");
		context.setData("pgsPanel", pgsPanel);
		context.setInput("pgsCategory","Body measurement"); //only PGS000027

		// run qc to create chunkfile

		InputValidation inputValidation = new InputValidationMock(configFolder);
		// run and test
		boolean result = run(context, inputValidation);
		assertTrue(result);

		QcStatisticsMock qcStats = new QcStatisticsMock(configFolder);
		result = run(context, qcStats);

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

		ZipFile zipFile = new ZipFile("test-data/tmp/pgs_output/scores.zip", PASSWORD.toCharArray());
		zipFile.extractAll("test-data/tmp");
		CsvTableReader readerExpected = new CsvTableReader("test-data/data/pgs/expected.txt", ',');
		CsvTableReader readerActual = new CsvTableReader("test-data/tmp/scores.txt", ',');
		assertEquals(2, readerActual.getColumns().length); //only sample and PGS000027

		while (readerExpected.next() && readerActual.next()) {
			assertEquals(readerExpected.getDouble("PGS000027"), readerActual.getDouble("PGS000027"), 0.00001);
		}
		readerExpected.close();
		readerActual.close();

		// check if html report file exisits
		new File("test-data/tmp/local/scores.html").exists();

		FileUtil.deleteDirectory("test-data/tmp");
		zipFile.close();

	}

	@Test
	public void testPipelineWithEagleAndScoresAndFormat() throws IOException, ZipException {

		String configFolder = "test-data/configs/hapmap-chr20";
		String inputFolder = "test-data/data/chr20-unphased";

		// import scores into hdfs
		String score1 = "test-data/data/prsweb/PRSWEB_PHECODE153_CRC-Huyghe_PT_UKB_20200608_WEIGHTS.txt";
		String format1 = "test-data/data/prsweb/PRSWEB_PHECODE153_CRC-Huyghe_PT_UKB_20200608_WEIGHTS.txt.format";

		String targetScore1 = HdfsUtil.path("scores-hdfs", "PRSWEB_PHECODE153_CRC-Huyghe_PT_UKB_20200608_WEIGHTS.txt");
		HdfsUtil.put(score1, targetScore1);

		String targetFormat1 = HdfsUtil.path("scores-hdfs",
				"PRSWEB_PHECODE153_CRC-Huyghe_PT_UKB_20200608_WEIGHTS.txt.format");
		HdfsUtil.put(format1, targetFormat1);

		// create workflow context and set scores
		WorkflowTestContext context = buildContext(inputFolder, "hapmap2");
		context.setOutput("outputScores", "cloudgene2-hdfs");

		Map<String, Object> pgsPanel = new HashMap<String, Object>();
		List<String> scores = new Vector<String>();
		scores.add("PRSWEB_PHECODE153_CRC-Huyghe_PT_UKB_20200608_WEIGHTS.txt");
		pgsPanel.put("location", "scores-hdfs");
		pgsPanel.put("scores", scores);
		pgsPanel.put("build", "hg19");
		context.setData("pgsPanel", pgsPanel);

		// run qc to create chunkfile

		InputValidation inputValidation = new InputValidationMock(configFolder);
		// run and test
		boolean result = run(context, inputValidation);
		assertTrue(result);

		QcStatisticsMock qcStats = new QcStatisticsMock(configFolder);
		result = run(context, qcStats);

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

		ZipFile zipFile = new ZipFile("test-data/tmp/local/chr_20.zip", PASSWORD.toCharArray());
		zipFile.extractAll("test-data/tmp");

		VcfFile file = VcfFileUtil.load("test-data/tmp/chr20.dose.vcf.gz", 100000000, false);

		assertEquals("20", file.getChromosome());
		assertEquals(51, file.getNoSamples());
		assertEquals(true, file.isPhased());
		assertEquals(TOTAL_REFPANEL_CHR20_B37, file.getNoSnps());

		int snpInInfo = getLineCount("test-data/tmp/chr20.info.gz");
		assertEquals(snpInInfo, file.getNoSnps());

		String[] args = { "test-data/tmp/chr20.dose.vcf.gz", "--ref", score1, "--out", "test-data/tmp/expected.txt" };
		int resultScore = new CommandLine(new ApplyScoreCommand()).execute(args);
		assertEquals(0, resultScore);

		zipFile = new ZipFile("test-data/tmp/pgs_output/scores.zip", PASSWORD.toCharArray());
		zipFile.extractAll("test-data/tmp");
		CsvTableReader readerExpected = new CsvTableReader("test-data/tmp/expected.txt", ',');
		CsvTableReader readerActual = new CsvTableReader("test-data/tmp/scores.txt", ',');

		while (readerExpected.next() && readerActual.next()) {
			assertEquals(readerExpected.getDouble("PRSWEB_PHECODE153_CRC-Huyghe_PT_UKB_20200608_WEIGHTS"),
					readerActual.getDouble("PRSWEB_PHECODE153_CRC-Huyghe_PT_UKB_20200608_WEIGHTS"), 0.00001);
		}
		readerExpected.close();
		readerActual.close();

		// check if html report file exisits
		new File("test-data/tmp/local/scores.html").exists();

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
	public void testPipelineWithBeaglePhasingOnly() throws IOException, ZipException {

		String configFolder = "test-data/configs/beagle";
		String inputFolder = "test-data/data/chr20-unphased";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, "hapmap2");

		context.setInput("mode", "phasing");
		context.setInput("phasing", "beagle");

		// run qc to create chunkfile
		QcStatisticsMock qcStats = new QcStatisticsMock(configFolder);
		boolean result = run(context, qcStats);

		assertTrue(result);
		assertTrue(context.hasInMemory("Remaining sites in total: 7,735"));

		// add panel to hdfs
		importRefPanel(FileUtil.path(configFolder, "ref-panels"));
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

		int snpInInfo = getLineCount("test-data/tmp/chr20.info.gz");
		assertEquals(snpInInfo, file.getNoSnps());

		FileUtil.deleteDirectory("test-data/tmp");

	}

	private int getLineCount(String filename) throws IOException {
		LineReader reader = new LineReader(filename);
		int lines = 0;
		while (reader.next()) {

			String line = reader.get();
			{
				if (!line.startsWith("#"))
					lines++;
			}
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

			if (!line.startsWith("#")) {
				String snp = line.split("\\s+")[1];
				if (Integer.valueOf(snp) <= pos) {
					return false;
				}
				pos = Integer.valueOf(snp);
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

		context.setInput("phasing", "");

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
		assertEquals(infoCount, file.getNoSnps());
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
		context.setInput("phasing", "eagle");
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

		context.setOutput("pgs_output", file.getAbsolutePath() + "/pgs_output");
		FileUtil.createDirectory(file.getAbsolutePath() + "/pgs_output");

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
