package genepi.imputationserver.steps;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import genepi.hadoop.common.WorkflowStep;
import genepi.imputationserver.util.WorkflowTestContext;
import genepi.io.FileUtil;
import genepi.io.text.LineReader;
import junit.framework.TestCase;

public class QCStatisticsTest extends TestCase {

	public static final boolean VERBOSE = true;

	public void testQcStatisticsOneChunk() throws IOException {

		String configFolder = "test-data/configs/hapmap-chr1";
		String inputFolder = "test-data/data/single";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, "hapmap2");
		
		//get output directory
		String out = context.getOutput("excludeLog");

		// create step instance
		QcStatisticsMock qcStats = new QcStatisticsMock(configFolder);

		// run and test
		boolean result = run(context, qcStats);

		assertFalse(result);

		// check statistics
		assertTrue(context.hasInMemory("Alternative allele frequency > 0.5 sites: 185"));
		assertTrue(context.hasInMemory("Excluded sites in total: 336"));
		assertTrue(context.hasInMemory("Remaining sites in total: 96"));
		assertTrue(context.hasInMemory("Monomorphic sites: 331"));

		FileUtil.deleteDirectory(new File(out));

	}

	public void testChunkExcluded() throws IOException {

		String configFolder = "test-data/configs/hapmap-chr1";
		String inputFolder = "test-data/data/single";
		
		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, "hapmap2");
		
		//get output directory
		String out = context.getOutput("excludeLog");

		// create step instance
		QcStatisticsMock qcStats = new QcStatisticsMock(configFolder);

		// run and test
		boolean result = run(context, qcStats);

		assertFalse(result);
		// check statistics
		assertTrue(context.hasInMemory("No chunks passed the QC step"));

		FileUtil.deleteDirectory(new File(out));

	}

	public void testQcStatisticsAllChunksFailed() throws IOException {

		String configFolder = "test-data/configs/hapmap-3chr";
		String inputFolder = "test-data/data/simulated-chip-3chr-qc";
		
		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, "hapmap2");

		//get output directory
		String out = context.getOutput("excludeLog");
		
		// create step instance
		QcStatisticsMock qcStats = new QcStatisticsMock(configFolder);

		// run and test
		boolean result = run(context, qcStats);

		assertFalse(result);

		// check statistics
		assertTrue(context.hasInMemory("Alternative allele frequency > 0.5 sites: 37,503"));
		assertTrue(context.hasInMemory("Duplicated sites: 618"));
		assertTrue(context.hasInMemory("36 Chunk(s) excluded"));
		assertTrue(context.hasInMemory("No chunks passed the QC step"));

		LineReader reader = new LineReader(FileUtil.path(out, "chunks-excluded.txt"));

		int count = 0;
		String testLine = null;

		while (reader.next()) {
			count++;
			if (count == 8) {
				testLine = reader.get();
			}
		}

		assertEquals(37, count);

		assertEquals("chunk_1_0120000001_0140000000" + "\t" + "108" + "\t" + "0.9391304347826087" + "\t" + "19",
				testLine);

		FileUtil.deleteDirectory(new File(out));
	}
	
	public void testQcStatisticsAllChunksPassed() throws IOException {

		String configFolder = "test-data/configs/hapmap-3chr";
		String inputFolder = "test-data/data/simulated-chip-3chr-imputation";
		
		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, "hapmap2");

		//get output directory
		String out = context.getOutput("excludeLog");
		
		// create step instance
		QcStatisticsMock qcStats = new QcStatisticsMock(configFolder);

		// run and test
		boolean result = run(context, qcStats);

		// check statistics
		assertTrue(context.hasInMemory("Excluded sites in total: 3,057"));
		assertTrue(context.hasInMemory("Remaining sites in total: 117,499"));
		LineReader reader = new LineReader(FileUtil.path(out, "chunks-excluded.txt"));

		int count = 0;

		while (reader.next()) {
			count++;
		}

		assertEquals(1, count);

		//FileUtil.deleteDirectory(new File(out));
	}

	class QcStatisticsMock extends QualityControlLocal {

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

	}

	protected boolean run(WorkflowTestContext context, WorkflowStep step) {
		step.setup(context);
		return step.run(context);
	}

	protected WorkflowTestContext buildContext(String folder, String refpanel) {
		WorkflowTestContext context = new WorkflowTestContext();
		
		File file = new File("test-data/tmp");
		file.mkdirs();

		context.setVerbose(VERBOSE);
		context.setInput("files", folder);
		context.setInput("population", "eur");
		context.setInput("refpanel", refpanel);
		context.setInput("chunksize", "20000000");
		context.setInput("phasingsize", "5000000");
		context.setOutput("outputmaf", file.getAbsolutePath() + "/test.txt");
		context.setOutput("mafchunkfile", file.getAbsolutePath());
		context.setOutput("excludeLog", file.getAbsolutePath());
		context.setOutput("statistics", file.getAbsolutePath());
		context.setOutput("chunks", file.getAbsolutePath());
		return context;

	}

}
