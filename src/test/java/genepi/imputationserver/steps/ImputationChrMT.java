package genepi.imputationserver.steps;

import static org.junit.Assert.assertEquals;
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
import net.lingala.zip4j.ZipFile;
import net.lingala.zip4j.exception.ZipException;

public class ImputationChrMT {

	public static final boolean VERBOSE = true;

	public static final String BINARIES_HDFS = "binaries";

	public static final String PASSWORD = "random-pwd";

	@BeforeClass
	public static void setUp() throws Exception {
		TestCluster.getInstance().start();
	}

	@AfterClass
	public static void tearDown() throws Exception {
		TestCluster.getInstance().stop();
	}

	@Test
	public void testChrMTPipeline() throws IOException, ZipException {

		String configFolder = "test-data/configs/phylotree-chrMT";
		String inputFolder = "test-data/data/chrMT";

		File file = new File("test-data/tmp");
		if (file.exists()) {
			FileUtil.deleteDirectory(file);
		}

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, "phylotree");
		
		// create step instance
		InputValidation inputValidation = new InputValidationMock(configFolder);

		// run and test
		boolean result = run(context, inputValidation);
		assertEquals(true, result);

		// run qc to create chunkfile
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

		ZipFile zipFile = new ZipFile("test-data/tmp/local/chr_MT.zip", PASSWORD.toCharArray());
	
		zipFile.extractAll("test-data/tmp");

		VcfFile vcfFile = VcfFileUtil.load("test-data/tmp/chrMT.dose.vcf.gz", 100000000, false);

		assertEquals("MT", vcfFile.getChromosome());
		assertEquals(5435, vcfFile.getNoSamples());
		FileUtil.deleteDirectory(file);

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
		context.setConfig("binaries", BINARIES_HDFS);
		context.setInput("password", PASSWORD);

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
