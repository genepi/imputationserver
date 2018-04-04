package genepi.imputationserver.steps;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;

import genepi.hadoop.common.WorkflowStep;
import genepi.imputationserver.steps.vcf.VcfFileUtil;
import genepi.imputationserver.util.WorkflowTestContext;
import genepi.io.FileUtil;
import genepi.io.text.LineReader;
import htsjdk.samtools.util.CloseableIterator;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;
import junit.framework.TestCase;
import net.lingala.zip4j.exception.ZipException;

public class FastQualityControlTest extends TestCase {

	// baseline for all tests was execution of running pipeline on Impuation
	// Server and compared to checkVCF

	public static final boolean VERBOSE = true;

	public void testQcStatistics() throws IOException {

		String configFolder = "test-data/configs/hapmap-chr1";
		String inputFolder = "test-data/data/single";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, "hapmap2");

		// create step instance
		FastQualityControlMock qcStats = new FastQualityControlMock(configFolder);

		// run and test
		boolean result = run(context, qcStats);

		assertFalse(result);

		// check statistics
		assertTrue(context.hasInMemory("Alternative allele frequency > 0.5 sites: 185"));
		assertTrue(context.hasInMemory("Excluded sites in total: 336"));
		assertTrue(context.hasInMemory("Remaining sites in total: 96"));
		assertTrue(context.hasInMemory("Monomorphic sites: 331"));

		FileUtil.deleteDirectory("test-data/tmp");

	}

	public void testQcStatisticAllChunksExcluded() throws IOException {

		String configFolder = "test-data/configs/hapmap-chr1";
		String inputFolder = "test-data/data/single";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, "hapmap2");

		// create step instance
		FastQualityControlMock qcStats = new FastQualityControlMock(configFolder);

		// run and test
		boolean result = run(context, qcStats);

		assertFalse(result);
		// check statistics
		assertTrue(context.hasInMemory("No chunks passed the QC step"));

		FileUtil.deleteDirectory("test-data/tmp");

	}

	public void testQcStatisticsAllChunksFailed() throws IOException {

		String configFolder = "test-data/configs/hapmap-3chr";
		String inputFolder = "test-data/data/simulated-chip-3chr-qc";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, "hapmap2");

		// create step instance
		FastQualityControlMock qcStats = new FastQualityControlMock(configFolder);

		// run and test
		boolean result = run(context, qcStats);

		assertFalse(result);

		// check statistics
		assertTrue(context.hasInMemory("Alternative allele frequency > 0.5 sites: 37,503"));
		assertTrue(context.hasInMemory("Duplicated sites: 618"));
		assertTrue(context.hasInMemory("36 Chunk(s) excluded"));
		assertTrue(context.hasInMemory("No chunks passed the QC step"));

		FileUtil.deleteDirectory("test-data/tmp");
	}

	public void testCountLinesInFailedChunkFile() throws IOException {

		String configFolder = "test-data/configs/hapmap-3chr";
		String inputFolder = "test-data/data/simulated-chip-3chr-qc";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, "hapmap2");

		String out = context.getOutput("statisticDir");

		// create step instance
		FastQualityControlMock qcStats = new FastQualityControlMock(configFolder);

		// run and test
		boolean result = run(context, qcStats);

		assertFalse(result);

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

		FileUtil.deleteDirectory("test-data/tmp");
	}

	public void testQcStatisticsAllChunksPassed() throws IOException {

		String configFolder = "test-data/configs/hapmap-3chr";
		String inputFolder = "test-data/data/simulated-chip-3chr-imputation";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, "hapmap2");

		// create step instance
		FastQualityControlMock qcStats = new FastQualityControlMock(configFolder);

		// run and test
		run(context, qcStats);

		// check statistics
		assertTrue(context.hasInMemory("Excluded sites in total: 3,057"));
		assertTrue(context.hasInMemory("Remaining sites in total: 117,499"));

		FileUtil.deleteDirectory("test-data/tmp");
	}

	public void testCountSitesForOneChunkedContig() throws IOException {

		String configFolder = "test-data/configs/hapmap-chr1";
		String inputFolder = "test-data/data/simulated-chip-1chr-imputation";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, "hapmap2");

		String out = context.getOutput("chunksDir");

		// create step instance
		FastQualityControlMock qcStats = new FastQualityControlMock(configFolder);

		// run and test
		assertTrue(run(context, qcStats));

		File[] files = new File(out).listFiles();
		Arrays.sort(files);

		// baseline from a earlier job execution
		int[] array = { 4588, 968, 3002, 5781, 5116, 4750, 5699, 5174, 6334, 3188, 5106, 5832, 5318 };
		int pos = 0;

		for (File file : files) {
			int count = 0;
			if (file.getName().endsWith(".gz")) {
				VCFFileReader vcfReader = new VCFFileReader(file, false);
				CloseableIterator<VariantContext> it = vcfReader.iterator();
				while (it.hasNext()) {
					it.next();
					count++;
				}
				assertEquals(array[pos], count);
				vcfReader.close();
				pos++;
			}

		}
		FileUtil.deleteDirectory(new File(out));
	}

	public void testCountAmountSplitsForSeveralContigs() throws IOException {

		String configFolder = "test-data/configs/hapmap-3chr";
		String inputFolder = "test-data/data/simulated-chip-3chr-imputation";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, "hapmap2");

		String out = context.getOutput("chunksDir");

		// create step instance
		FastQualityControlMock qcStats = new FastQualityControlMock(configFolder);

		// run and test
		run(context, qcStats);

		File[] files = new File(out).listFiles();
		Arrays.sort(files);

		// baseline from a earlier job execution

		int count = 0;
		for (File file : files) {
			if (file.getName().endsWith(".gz")) {
				count++;
			}

		}
		// https://genome.ucsc.edu/goldenpath/help/hg19.chrom.sizes
		assertEquals(13 + 13 + 10, count);

		FileUtil.deleteDirectory(new File(out));
	}

	public void testCountLinesInChunkMetaFile() throws IOException {

		String configFolder = "test-data/configs/hapmap-chr1";
		String inputFolder = "test-data/data/simulated-chip-1chr-imputation";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, "hapmap2");

		// get output directory
		String out = context.getOutput("chunkFileDir");

		// create step instance
		FastQualityControlMock qcStats = new FastQualityControlMock(configFolder);

		// run and test
		run(context, qcStats);

		LineReader reader = new LineReader(FileUtil.path(out, "1"));

		int count = 0;
		while (reader.next()) {
			count++;
		}

		assertEquals(13, count);

		FileUtil.deleteDirectory(new File(out));
	}

	public void testCountSamplesInCreatedChunk() throws IOException {

		String configFolder = "test-data/configs/hapmap-chr1";
		String inputFolder = "test-data/data/simulated-chip-1chr-imputation";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, "hapmap2");

		// get output directory
		String out = context.getOutput("chunksDir");

		// create step instance
		FastQualityControlMock qcStats = new FastQualityControlMock(configFolder);

		// run and test
		assertTrue(run(context, qcStats));
		for (File file : new File(out).listFiles()) {
			if (file.getName().endsWith("chunk_1_80000001_100000000.vcf.gz")) {
				VCFFileReader vcfReader = new VCFFileReader(file, false);
				CloseableIterator<VariantContext> it = vcfReader.iterator();
				if (it.hasNext()) {
					VariantContext a = it.next();
					assertEquals(255, a.getNSamples());
				}
				vcfReader.close();
			}

		}

		FileUtil.deleteDirectory(new File(out));
	}

	@Test
	public void testMonomorphicSnps() throws IOException {

		String configFolder = "test-data/configs/hapmap-chr20";
		String inputFolder1 = "test-data/data/chr20-phased-1sample";
		String inputFolder50 = "test-data/data/chr20-phased";
		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder1, "hapmap2");

		// create step instance
		FastQualityControlMock qcStats = new FastQualityControlMock(configFolder);

		boolean result = run(context, qcStats);

		assertTrue(result);
		assertTrue(context.hasInMemory("Monomorphic sites: 0"));

		context = buildContext(inputFolder50, "hapmap2");
		result = run(context, qcStats);
		assertTrue(result);
		assertTrue(context.hasInMemory("Monomorphic sites: 11"));

		FileUtil.deleteDirectory("test-data/tmp");

	}

	@Test
	public void testChrXSplits() throws IOException, ZipException {

		String configFolder = "test-data/configs/hapmap-chrX";
		String inputFolder = "test-data/data/chrX-unphased";

		File file = new File("test-data/tmp");
		if (file.exists()) {
			FileUtil.deleteDirectory(file);
		}

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, "phase1");

		// run qc to create chunkfile
		FastQualityControlMock qcStats = new FastQualityControlMock(configFolder);
		boolean result = run(context, qcStats);

		assertTrue(result);

		String out = context.getOutput("chunksDir");
		int count = 0;
		for (File file2 : new File(out).listFiles()) {
			if (file2.getName().endsWith("vcf.gz")) {
				count++;
			}
		}

		assertEquals(13, count);

		FileUtil.deleteDirectory(file);

	}

	
	@Test
	public void testChrXMixedGenotypes() throws IOException, ZipException {

		String configFolder = "test-data/configs/hapmap-chrX";
		String inputFolder = "test-data/data/chrX-unphased-mixed";

		File file = new File("test-data/tmp");
		if (file.exists()) {
			FileUtil.deleteDirectory(file);
		}

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, "phase1");

		// run qc to create chunkfile
		FastQualityControlMock qcStats = new FastQualityControlMock(configFolder);
		boolean result = run(context, qcStats);

		assertFalse(result);
		assertTrue(context.hasInMemory("Chromosome X nonPAR region includes > 10 % mixed genotypes."));

		FileUtil.deleteDirectory(file);

	}
	
	@Test
	public void testChrXPloidyError() throws IOException, ZipException {

		String configFolder = "test-data/configs/hapmap-chrX";
		String inputFolder = "test-data/data/chrX-unphased-ploidy";

		File file = new File("test-data/tmp");
		if (file.exists()) {
			FileUtil.deleteDirectory(file);
		}

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, "phase1");

		// run qc to create chunkfile
		FastQualityControlMock qcStats = new FastQualityControlMock(configFolder);
		boolean result = run(context, qcStats);

		assertFalse(result);
		assertTrue(context.hasInMemory("ChrX nonPAR region includes ambiguous samples"));

		FileUtil.deleteDirectory(file);

	}
	
	class FastQualityControlMock extends FastQualityControl {

		private String folder;

		public FastQualityControlMock(String folder) {
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
		context.setOutput("mafFile", file.getAbsolutePath() + "/maffile.txt");
		context.setOutput("chunkFileDir", file.getAbsolutePath());
		context.setOutput("statisticDir", file.getAbsolutePath());
		context.setOutput("chunksDir", file.getAbsolutePath());
		return context;

	}

}
