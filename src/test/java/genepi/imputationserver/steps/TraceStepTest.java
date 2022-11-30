package genepi.imputationserver.steps;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import cloudgene.sdk.internal.WorkflowStep;
import genepi.hadoop.HdfsUtil;
import genepi.imputationserver.steps.ancestry.TraceStep;
import genepi.imputationserver.steps.vcf.VcfFileUtil;
import genepi.imputationserver.util.TestCluster;
import genepi.imputationserver.util.WorkflowTestContext;
import genepi.io.FileUtil;

public class TraceStepTest {

	public static final boolean VERBOSE = true;

	public static final String BINARIES_HDFS = "binaries";
	
	public static final String REFERENCES_HDFS = "references";

	@BeforeClass
	public static void setUp() throws Exception {
		TestCluster.getInstance().start();
	}

	@AfterClass
	public static void tearDown() throws Exception {
		TestCluster.getInstance().stop();
	}

	@Test
	public void testTraceStep() throws IOException {

		String configFolder = "test-data/configs/hapmap-chr20";
		String inputFolder = "test-data/data/chr22.hg19";

		importBinaries("files/bin");
		importReferences("files/references");
		
		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, "hg19");

		// create step instance
		WorkflowStep step = new TraceStepMock(configFolder);
		// run and test
		boolean result = run(context, step);

		assertTrue(result);
	}

	@Test
	public void testTraceStepHg38() throws IOException {

		String configFolder = "test-data/configs/hapmap-chr20";
		String inputFolder = "test-data/data/chr22.hg38";

		importBinaries("files/bin");
		importReferences("files/references");
		
		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, "hg38");

		// create step instance
		WorkflowStep step = new TraceStepMock(configFolder);

		// run and test
		boolean result = run(context, step);

		assertTrue(result);
	}

	
	class TraceStepMock extends TraceStep {

		private String folder;

		public TraceStepMock(String folder) {
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

	protected boolean run(WorkflowTestContext context, WorkflowStep step) {
		step.setup(context);
		return step.run(context);
	}

	protected WorkflowTestContext buildContext(String folder, String build) {
		WorkflowTestContext context = new WorkflowTestContext();

		File file = new File("test-data/tmp");
		if (file.exists()) {
			FileUtil.deleteDirectory(file);
		}
		file.mkdirs();

		context.setVerbose(VERBOSE);
		context.setInput("files", folder);
		context.setInput("build", build);
		context.setInput("batch_size", "20");
		context.setInput("dim", "3");
		context.setInput("dim_high", "20");
		context.setInput("reference", "HGDP_238_chr22");
		context.setOutput("pgs_output", "pgs_output");
		FileUtil.deleteDirectory("pgs_output");
		FileUtil.createDirectory("pgs_output");

		context.setOutput("study_pc", "studypc.txt");
		context.setOutput("study_population", "study_population.txt");

		context.setConfig("references", REFERENCES_HDFS);
		context.setConfig("binaries", BINARIES_HDFS);

		context.setHdfsTemp("minimac-temp");
		HdfsUtil.createDirectory(context.getHdfsTemp());

		context.setOutput("trace_batches", "trace_batches");
		FileUtil.deleteDirectory("trace_batches");
		FileUtil.createDirectory("trace_batches");

		context.setLocalTemp("local-temp");
		FileUtil.deleteDirectory("local-temp");
		FileUtil.createDirectory("local-temp");

		return context;

	}
	
	private void importReferences(String folder) {
		System.out.println("Import References to " + REFERENCES_HDFS);
		String[] files = FileUtil.getFiles(folder, "*.*");
		HdfsUtil.delete(REFERENCES_HDFS);
		for (String file : files) {
			String target = HdfsUtil.path(REFERENCES_HDFS, FileUtil.getFilename(file));
			System.out.println("  Import " + file + " to " + target);
			HdfsUtil.put(file, target);
		}
	}

	private void importBinaries(String folder) {
		System.out.println("Import Binaries to " + BINARIES_HDFS);
		String[] files = FileUtil.getFiles(folder, "*.*");
		HdfsUtil.delete(BINARIES_HDFS);
		for (String file : files) {
			String target = HdfsUtil.path(BINARIES_HDFS, FileUtil.getFilename(file));
			System.out.println("  Import " + file + " to " + target);
			HdfsUtil.put(file, target);
		}
	}

}
