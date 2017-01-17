package genepi.imputationserver.steps;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import genepi.hadoop.common.WorkflowStep;
import genepi.imputationserver.util.WorkflowTestContext;
import junit.framework.TestCase;

public class QCStatisticsTest extends TestCase {

	public static final boolean VERBOSE = true;

	public void testSnpStatistics() throws IOException {

		String configFolder = "test-data/configs/hapmap-chr1";
		String inputFolder = "test-data/data/single";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, "hapmap2");

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

	}

	public void testChunkExklusion() throws IOException {

		String configFolder = "test-data/configs/hapmap-chr1";
		String inputFolder = "test-data/data/single";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, "hapmap2");

		// create step instance
		QcStatisticsMock qcStats = new QcStatisticsMock(configFolder);

		// run and test
		boolean result = run(context, qcStats);

		assertFalse(result);
		// check statistics
		assertTrue(context.hasInMemory("No chunks passed the QC step"));

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
			context.setInput("chunksize", "10000000");
			context.setInput("phasingsize", "5000000");
			context.setOutput("outputmaf", file.getAbsolutePath() + "/test.txt");
			context.setOutput("excludeLog", file.getAbsolutePath());
			context.setOutput("statistics", file.getAbsolutePath());
			context.setOutput("chunks", file.getAbsolutePath());
		
		return context;

	}

}
