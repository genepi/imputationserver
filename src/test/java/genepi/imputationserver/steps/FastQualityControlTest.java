package genepi.imputationserver.steps;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;

import com.esotericsoftware.yamlbeans.YamlException;

import genepi.imputationserver.BaseTestCase;
import genepi.imputationserver.util.WorkflowTestContext;
import genepi.io.FileUtil;
import genepi.io.text.LineReader;
import htsjdk.samtools.util.CloseableIterator;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;
import net.lingala.zip4j.exception.ZipException;

public class FastQualityControlTest extends BaseTestCase {

	// baseline for all tests was execution of running pipeline on Impuation
	// Server and compared to checkVCF

	public static final boolean VERBOSE = true;

	public void testQcStatistics() throws IOException {

		String configFolder = "test-data/configs/hapmap-chr1";
		String inputFolder = "test-data/data/single";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, configFolder, "hapmap2");

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

	}

	public void testQcStatisticAllChunksExcluded() throws IOException {

		String configFolder = "test-data/configs/hapmap-chr1";
		String inputFolder = "test-data/data/single";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, configFolder, "hapmap2");

		// create step instance
		FastQualityControlMock qcStats = new FastQualityControlMock(configFolder);

		// run and test
		boolean result = run(context, qcStats);

		assertFalse(result);
		// check statistics
		assertTrue(context.hasInMemory("No chunks passed the QC step"));

	}

	public void testQcStatisticsAllChunksFailed() throws IOException {

		String configFolder = "test-data/configs/hapmap-3chr";
		String inputFolder = "test-data/data/simulated-chip-3chr-qc";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, configFolder, "hapmap2");

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

	}

	public void testCountLinesInFailedChunkFile() throws IOException {

		String configFolder = "test-data/configs/hapmap-3chr";
		String inputFolder = "test-data/data/simulated-chip-3chr-qc";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, configFolder, "hapmap2");

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

	}

	public void testQcStatisticsAllChunksPassed() throws IOException {

		String configFolder = "test-data/configs/hapmap-3chr";
		String inputFolder = "test-data/data/simulated-chip-3chr-imputation";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, configFolder, "hapmap2");

		// create step instance
		FastQualityControlMock qcStats = new FastQualityControlMock(configFolder);

		// run and test
		run(context, qcStats);

		// check statistics
		assertTrue(context.hasInMemory("Excluded sites in total: 3,058"));
		assertTrue(context.hasInMemory("Remaining sites in total: 117,498"));

	}

	public void testCountSitesForOneChunkedContig() throws IOException {

		String configFolder = "test-data/configs/hapmap-chr1";
		String inputFolder = "test-data/data/simulated-chip-1chr-imputation";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, configFolder, "hapmap2");

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

	}

	public void testCountAmountSplitsForSeveralContigs() throws IOException {

		String configFolder = "test-data/configs/hapmap-3chr";
		String inputFolder = "test-data/data/simulated-chip-3chr-imputation";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, configFolder, "hapmap2");

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

	}

	public void testCountLinesInChunkMetaFile() throws IOException {

		String configFolder = "test-data/configs/hapmap-chr1";
		String inputFolder = "test-data/data/simulated-chip-1chr-imputation";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, configFolder, "hapmap2");

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

	}

	public void testCountSamplesInCreatedChunk() throws IOException {

		String configFolder = "test-data/configs/hapmap-chr1";
		String inputFolder = "test-data/data/simulated-chip-1chr-imputation";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, configFolder, "hapmap2");

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

	}

	@Test
	public void testMonomorphicSnps() throws IOException {

		String configFolder = "test-data/configs/hapmap-chr20";
		String inputFolder1 = "test-data/data/chr20-phased-1sample";
		String inputFolder51 = "test-data/data/chr20-phased";
		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder1, configFolder, "hapmap2");

		// create step instance
		FastQualityControlMock qcStats = new FastQualityControlMock(configFolder);

		boolean result = run(context, qcStats);

		assertTrue(result);
		assertTrue(context.hasInMemory("Monomorphic sites: 0"));

		context = buildContext(inputFolder51, configFolder, "hapmap2");
		result = run(context, qcStats);
		assertTrue(result);
		assertTrue(context.hasInMemory("Monomorphic sites: 11"));

	}

		
	@Test
	public void testMonomorphicSnpsAndFilter() throws IOException {

		String configFolder = "test-data/configs/hapmap-chr20";
		String inputFolder51 = "test-data/data/chr20-phased";
		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder51, configFolder, "hapmap2-monomorphic-filter-51");

		// create step instance
		FastQualityControlMock qcStats = new FastQualityControlMock(configFolder);

		boolean result = run(context, qcStats);

		assertTrue(result);
		assertTrue(context.hasInMemory("Monomorphic sites: 11"));
		assertTrue(context.hasInMemory("Remaining sites in total: 7,735"));

		context = buildContext(inputFolder51, configFolder, "hapmap2-monomorphic-filter-52");
		result = run(context, qcStats);
		assertTrue(result);
		assertTrue(context.hasInMemory("Monomorphic sites: 0"));
		assertTrue(context.hasInMemory("Remaining sites in total: 7,746"));

	}
	
	@Test
	public void testChrXSplits() throws IOException, ZipException {

		String configFolder = "test-data/configs/hapmap-chrX";
		String inputFolder = "test-data/data/chrX-unphased";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, configFolder, "phase1");

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

	}

	@Test
	public void testChrXInvalidAlleles() throws IOException, ZipException {

		String configFolder = "test-data/configs/hapmap-chrX";
		String inputFolder = "test-data/data/chrX-phased-invalid";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, configFolder, "phase1");

		// run qc to create chunkfile
		FastQualityControlMock qcStats = new FastQualityControlMock(configFolder);
		boolean result = run(context, qcStats);

		assertTrue(result);
		assertTrue(context.hasInMemory("Invalid alleles: 190"));

	}

	@Test
	public void testChrXMixedGenotypes() throws IOException, ZipException {

		String configFolder = "test-data/configs/hapmap-chrX";
		String inputFolder = "test-data/data/chrX-unphased-mixed";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, configFolder, "phase1");

		// run qc to create chunkfile
		FastQualityControlMock qcStats = new FastQualityControlMock(configFolder);
		boolean result = run(context, qcStats);

		assertFalse(result);
		assertTrue(context.hasInMemory("Chromosome X nonPAR region includes > 10 % mixed genotypes."));

	}

	@Test
	public void testChrXPloidyError() throws IOException, ZipException {

		String configFolder = "test-data/configs/hapmap-chrX";
		String inputFolder = "test-data/data/chrX-unphased-ploidy";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, configFolder, "phase1");

		// run qc to create chunkfile
		FastQualityControlMock qcStats = new FastQualityControlMock(configFolder);
		boolean result = run(context, qcStats);

		assertFalse(result);
		assertTrue(context.hasInMemory("ChrX nonPAR region includes ambiguous samples"));

	}

	@Test
	public void testAlleleFrequencyCheckWithWrongPopulation() throws YamlException, FileNotFoundException {
		String configFolder = "test-data/configs/hapmap-chr1";
		String inputFolder = "test-data/data/single";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, configFolder, "hapmap2");
		context.setInput("population", "afr");

		// create step instance
		FastQualityControlMock qcStats = new FastQualityControlMock(configFolder);

		// run and test
		boolean result = run(context, qcStats);

		assertFalse(result);

		// check statistics
		assertTrue(context.hasInMemory("Population 'afr' is not supported by reference panel 'hapmap2'."));
	}

	@Test
	public void testAlleleFrequencyCheckWithNoSamplesForPopulation() throws YamlException, FileNotFoundException {

		String configFolder = "test-data/configs/hapmap-3chr";
		String inputFolder = "test-data/data/simulated-chip-3chr-imputation";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, configFolder, "hapmap2");
		context.setInput("population", "mixed");
		// create step instance
		FastQualityControlMock qcStats = new FastQualityControlMock(configFolder);

		// run and test
		boolean result = run(context, qcStats);

		assertTrue(result);

		// check statistics
		assertTrue(context.hasInMemory("[WARN] Skip allele frequency check."));
	}

	public void testQcStatisticsAllowStrandFlips() throws IOException {

		String configFolder = "test-data/configs/hapmap-3chr";
		String inputFolder = "test-data/data/simulated-chip-3chr-imputation";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, configFolder, "hapmap2");

		// create step instance
		FastQualityControlMock qcStats = new FastQualityControlMock(configFolder);

		// run and test
		run(context, qcStats);

		// check statistics
		assertTrue(context.hasInMemory("Excluded sites in total: 3,058"));
		assertTrue(context.hasInMemory("Remaining sites in total: 117,498"));

	}

	public void testQcStatisticsDontAllowStrandFlips() throws IOException {

		String configFolder = "test-data/configs/hapmap-3chr";
		String inputFolder = "test-data/data/simulated-chip-3chr-imputation";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, configFolder, "hapmap2-qcfilter-strandflips");

		// create step instance
		FastQualityControlMock qcStats = new FastQualityControlMock(configFolder);

		// run and test
		run(context, qcStats);

		// check statistics
		assertTrue(context.hasInMemory("Excluded sites in total: 3,058"));
		assertTrue(context.hasInMemory("Remaining sites in total: 117,498"));
		assertTrue(context.hasInMemory(
				"<b>Error:</b> More than -1 obvious strand flips have been detected. Please check strand. Imputation cannot be started!"));

	}

	public void testQcStatisticsFilterOverlap() throws IOException {

		String configFolder = "test-data/configs/hapmap-3chr";
		String inputFolder = "test-data/data/simulated-chip-3chr-imputation";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, configFolder, "hapmap2-qcfilter-ref-overlap");

		// create step instance
		FastQualityControlMock qcStats = new FastQualityControlMock(configFolder);

		// run and test
		run(context, qcStats);

		// check statistics
		assertTrue(context.hasInMemory(
				"<b>Warning:</b> 36 Chunk(s) excluded: reference overlap < 99.0% (see [NOT AVAILABLE] for details)"));

	}

	public void testQcStatisticsFilterMinSnps() throws IOException {

		String configFolder = "test-data/configs/hapmap-3chr";
		String inputFolder = "test-data/data/simulated-chip-3chr-imputation";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, configFolder, "hapmap2-qcfilter-min-snps");

		// create step instance
		FastQualityControlMock qcStats = new FastQualityControlMock(configFolder);

		// run and test
		run(context, qcStats);

		// check statistics
		assertTrue(context
				.hasInMemory("<b>Warning:</b> 2 Chunk(s) excluded: < 1000 SNPs (see [NOT AVAILABLE]  for details)."));

	}

	public void testQcStatisticsFilterSampleCallrate() throws IOException {

		String configFolder = "test-data/configs/hapmap-3chr";
		String inputFolder = "test-data/data/simulated-chip-3chr-imputation";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, configFolder, "hapmap2-qcfilter-low-callrate");

		// create step instance
		FastQualityControlMock qcStats = new FastQualityControlMock(configFolder);

		// run and test
		run(context, qcStats);

		// check statistics
		assertTrue(context.hasInMemory(
				"<b>Warning:</b> 36 Chunk(s) excluded: at least one sample has a call rate < 101.0% (see [NOT AVAILABLE] for details)"));

	}

	@Test
	public void testChr23PipelineLifting() throws IOException, ZipException {

		String configFolder = "test-data/configs/hapmap-chrX-hg38";
		String inputFolder = "test-data/data/chr23-unphased";

		// maybe git large files?
		if (!new File(
				"test-data/configs/hapmap-chrX-hg38/ref-panels/ALL.X.nonPAR.phase1_v3.snps_indels_svs.genotypes.all.noSingleton.recode.hg38.bcf")
						.exists()) {
			System.out.println("chrX bcf nonPAR file not available");
			return;
		}

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, configFolder, "hapmap2");

		// run qc to create chunkfile
		FastQualityControlMock qcStats = new FastQualityControlMock(configFolder);
		boolean result = run(context, qcStats);

		assertTrue(result);
		assertTrue(context.hasInMemory("Alternative allele frequency > 0.5 sites: 8,973"));
		assertTrue(context.hasInMemory("[MESSAGE] [WARN] Excluded sites in total: 18,076"));

	}

	@Test
	public void testChrXPipelineLifting() throws IOException, ZipException {

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
		WorkflowTestContext context = buildContext(inputFolder, configFolder, "hapmap2");

		// run qc to create chunkfile
		FastQualityControlMock qcStats = new FastQualityControlMock(configFolder);
		boolean result = run(context, qcStats);

		assertTrue(result);
		assertTrue(context.hasInMemory("Alternative allele frequency > 0.5 sites: 8,973"));
		assertTrue(context.hasInMemory("[MESSAGE] [WARN] Excluded sites in total: 18,076"));

	}

	public void testRegionImputationSimple() throws IOException {

		String configFolder = "test-data/configs/hapmap-chr1";
		String inputFolder = "test-data/data/simulated-chip-1chr-imputation";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, configFolder, "hapmap2-region-simple");

		// create step instance
		FastQualityControlMock qcStats = new FastQualityControlMock(configFolder);

		// run and test
		run(context, qcStats);

		// check statistics
		assertTrue(context.hasInMemory("Remaining sites in total: 1"));

	}

	public void testRegionImputationComplex() throws IOException {

		String configFolder = "test-data/configs/hapmap-chr1";
		String inputFolder = "test-data/data/simulated-chip-1chr-imputation";

		// create workflow context
		WorkflowTestContext context = buildContext(inputFolder, configFolder, "hapmap2-region-complex");

		// create step instance
		FastQualityControlMock qcStats = new FastQualityControlMock(configFolder);

		// run and test
		run(context, qcStats);

		// check statistics
		assertTrue(context.hasInMemory("Remaining sites in total: 2"));

	}

}
