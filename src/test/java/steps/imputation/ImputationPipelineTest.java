package steps.imputation;

import genepi.imputationserver.steps.imputationMinimac3.ImputationPipelineMinimac3;
import genepi.imputationserver.steps.vcf.VcfChunk;
import genepi.imputationserver.steps.vcf.VcfChunkOutput;
import genepi.imputationserver.steps.vcf.VcfFile;
import genepi.imputationserver.steps.vcf.VcfFileUtil;
import genepi.io.FileUtil;
import genepi.io.text.LineReader;

import java.io.IOException;

import org.broadinstitute.variant.vcf.VCFUtils;

import junit.framework.TestCase;

public class ImputationPipelineTest extends TestCase {

	private ImputationPipelineMinimac3 pipeline;

	public static final int DEFAULT_PHASING_WINDOW = 1000000;
	public static final int DEFAULT_MINIMAC_WINDOW = 500000;
	public static final int DEFAULT_ROUNDS = 0;
	public static final String TEMP_FOLDER = "temp";

	public static final String REF_PHASE1 = "$chr.1000g.Phase1.v3.With.Parameter.Estimates.m3vcf.gz";

	public static final String REF_PHASE3 = "$chr.1000g.Phase3.v5.With.Parameter.Estimates.m3vcf.gz";

	@Override
	protected void setUp() throws Exception {

		String binaries = "files/minimac/bin";

		// config pipeline
		pipeline = new ImputationPipelineMinimac3();
		pipeline.setMinimacCommand(FileUtil.path(binaries, "Minimac3"));
		pipeline.setHapiUrCommand(FileUtil.path(binaries, "hapi-ur"));
		pipeline.setVcfCookerCommand(FileUtil.path(binaries, "vcfCooker"));
		pipeline.setVcf2HapCommand(FileUtil.path(binaries, "vcf2hap"));
		pipeline.setShapeItCommand(FileUtil.path(binaries, "shapeit"));
		pipeline.setHapiUrPreprocessCommand(FileUtil.path(binaries,
				"insert-map.pl"));

		String refFilename = "test-data";

		String mapShapeITPattern = "genetic_map_chr$chr_combined_b37.txt";
		String mapShapeITFilename = "test-data";

		String mapHapiURFilename = "genetic_map_chr$chr_combined_hapiur_b37.txt";
		String mapHapiURPattern = "test-data";

		pipeline.setPhasingWindow(DEFAULT_PHASING_WINDOW);
		pipeline.setRounds(DEFAULT_ROUNDS);
		pipeline.setMinimacWindow(DEFAULT_MINIMAC_WINDOW);
		pipeline.setRefFilename(refFilename);
		pipeline.setPattern(REF_PHASE1);
		pipeline.setMapShapeITPattern(mapShapeITPattern);
		pipeline.setMapShapeITFilename(mapShapeITFilename);
		pipeline.setMapHapiURFilename(mapHapiURFilename);
		pipeline.setMapHapiURPattern(mapHapiURPattern);

		pipeline.setPopulation("EUR");
		pipeline.setPhasing("shapeit");

		
		FileUtil.deleteDirectory(TEMP_FOLDER);

		FileUtil.createDirectory(TEMP_FOLDER);

	}

	@Override
	protected void tearDown() throws Exception {
		// FileUtil.deleteDirectory(TEMP_FOLDER);
	}

	public void testPhasedVcfFilePhase1() throws InterruptedException,
			IOException {

		FileUtil.createDirectory(TEMP_FOLDER);

		// chunk
		VcfChunk chunk = new VcfChunk();
		chunk.setChromosome("1");
		chunk.setStart(1);
		chunk.setEnd(10000000);
		chunk.setPhased(true);
		chunk.setVcfFilename("test-data/chr1.phased.vcf");

		pipeline.setPattern(REF_PHASE1);

		VcfChunkOutput outputChunk = new VcfChunkOutput(chunk, TEMP_FOLDER);
		outputChunk.setVcfFilename(chunk.getVcfFilename());

		assertTrue(pipeline.execute(chunk, outputChunk));

		// simple phased check of shapeit output
		String content = FileUtil.readFileAsString(outputChunk
				.getPhasedVcfFilename());
		assertFalse(content.contains("/"));
		assertTrue(content.contains("|"));

		// simple output check
		LineReader reader = new LineReader(outputChunk.getImputedVcfFilename());
		while (reader.next()) {
			if (reader.get().contains("nan")) {
				System.out.println(reader.get());
				// assertFalse(true);
			}
		}
		reader.close();

		FileUtil.deleteDirectory(TEMP_FOLDER);

	}

	public void testUnphasedChrX() throws InterruptedException, IOException {

		FileUtil.createDirectory(TEMP_FOLDER);

		VcfFileUtil.setBinaries("files/minimac/bin");
		
		VcfFile file = VcfFileUtil.load("test-data/gckd.small.vcf.gz", 10000000,true);
		
		VcfFileUtil.prepareChrX(file);
		
		// chunk
		VcfChunk chunk = new VcfChunk();
		chunk.setChromosome("X_auto");
		chunk.setStart(1);
		chunk.setEnd(10000000);
		chunk.setPhased(false);
		chunk.setVcfFilename("test-data/gckd.small.vcf.gz.auto.vcf.gz");

		pipeline.setPattern(REF_PHASE1);
		pipeline.setPhasing("shapeit");


		VcfChunkOutput outputChunk = new VcfChunkOutput(chunk, TEMP_FOLDER);
		outputChunk.setVcfFilename(chunk.getVcfFilename());

		assertTrue(pipeline.execute(chunk, outputChunk));

		// simple phased check of shapeit output
		String content = FileUtil.readFileAsString(outputChunk
				.getPhasedVcfFilename());
		assertFalse(content.contains("/"));
		assertTrue(content.contains("|"));

		// simple output check
		LineReader reader = new LineReader(outputChunk.getImputedVcfFilename());
		while (reader.next()) {
			if (reader.get().contains("nan")) {
				System.out.println(reader.get());
				// assertFalse(true);
			}
		}
		reader.close();

		FileUtil.deleteDirectory(TEMP_FOLDER);

	}
	
	public void testPhasedVcfFilePhase3() throws InterruptedException,
			IOException {

		FileUtil.createDirectory(TEMP_FOLDER);

		// chunk
		VcfChunk chunk = new VcfChunk();
		chunk.setChromosome("1");
		chunk.setStart(1);
		chunk.setEnd(10000000);
		chunk.setPhased(true);
		chunk.setVcfFilename("test-data/chr1.phased.vcf");

		pipeline.setPattern(REF_PHASE3);

		VcfChunkOutput outputChunk = new VcfChunkOutput(chunk, TEMP_FOLDER);
		outputChunk.setVcfFilename(chunk.getVcfFilename());

		assertTrue(pipeline.execute(chunk, outputChunk));

		// simple phased check of shapeit output
		String content = FileUtil.readFileAsString(outputChunk
				.getPhasedVcfFilename());
		assertFalse(content.contains("/"));
		assertTrue(content.contains("|"));

		// simple output check
		LineReader reader = new LineReader(outputChunk.getImputedVcfFilename());
		while (reader.next()) {
			assertFalse(reader.get().contains("nan"));
		}
		reader.close();

		// FileUtil.deleteDirectory(TEMP_FOLDER);

	}

	public void testUnPhasedVcfFileShapeItPhase1() throws InterruptedException,
			IOException {

		FileUtil.createDirectory(TEMP_FOLDER);

		// chunk
		VcfChunk chunk = new VcfChunk();
		chunk.setChromosome("1");
		chunk.setStart(1);
		chunk.setEnd(10000000);
		chunk.setPhased(false);
		chunk.setVcfFilename("test-data/chr1.unphased.vcf");

		pipeline.setPattern(REF_PHASE1);
		pipeline.setPhasing("shapeit");

		VcfChunkOutput outputChunk = new VcfChunkOutput(chunk, TEMP_FOLDER);
		outputChunk.setVcfFilename(chunk.getVcfFilename());

		assertTrue(pipeline.execute(chunk, outputChunk));

		// simple phased check of shapeit output
		String content = FileUtil.readFileAsString(outputChunk
				.getPhasedVcfFilename());
		assertFalse(content.contains("/"));
		assertTrue(content.contains("|"));

		// simple output check
		LineReader reader = new LineReader(outputChunk.getImputedVcfFilename());
		while (reader.next()) {
			if (reader.get().contains("nan")) {
				System.out.println(reader.get());
				// assertFalse(true);
			}
		}
		reader.close();

		// FileUtil.deleteDirectory(TEMP_FOLDER);

	}

	public void testUnPhasedVcfFileShapeItPhase3() throws InterruptedException,
			IOException {

		FileUtil.createDirectory(TEMP_FOLDER);

		// chunk
		VcfChunk chunk = new VcfChunk();
		chunk.setChromosome("1");
		chunk.setStart(1);
		chunk.setEnd(10000000);
		chunk.setPhased(false);
		chunk.setVcfFilename("test-data/chr1.unphased.vcf");

		pipeline.setPattern(REF_PHASE3);
		pipeline.setPhasing("shapeit");

		VcfChunkOutput outputChunk = new VcfChunkOutput(chunk, TEMP_FOLDER);
		outputChunk.setVcfFilename(chunk.getVcfFilename());

		assertTrue(pipeline.execute(chunk, outputChunk));

		// simple phased check of shapeit output
		String content = FileUtil.readFileAsString(outputChunk
				.getPhasedVcfFilename());
		assertFalse(content.contains("/"));
		assertTrue(content.contains("|"));

		// simple output check
		LineReader reader = new LineReader(outputChunk.getImputedVcfFilename());
		while (reader.next()) {
			assertFalse("", reader.get().contains("nan"));
		}
		reader.close();

		FileUtil.deleteDirectory(TEMP_FOLDER);

	}

	public void testUnPhasedVcfFileHapiURPhase1() throws InterruptedException,
			IOException {

		FileUtil.createDirectory(TEMP_FOLDER);

		// chunk
		VcfChunk chunk = new VcfChunk();
		chunk.setChromosome("1");
		chunk.setStart(1);
		chunk.setEnd(10000000);
		chunk.setPhased(false);
		chunk.setVcfFilename("test-data/chr1.unphased.vcf");

		pipeline.setPattern(REF_PHASE1);
		pipeline.setPhasing("shapeit");

		VcfChunkOutput outputChunk = new VcfChunkOutput(chunk, TEMP_FOLDER);
		outputChunk.setVcfFilename(chunk.getVcfFilename());

		assertTrue(pipeline.execute(chunk, outputChunk));

		// simple phased check of hapi-ur output
		String content = FileUtil.readFileAsString(outputChunk
				.getPhasedVcfFilename());
		assertFalse(content.contains("/"));
		assertTrue(content.contains("|"));

		FileUtil.deleteDirectory(TEMP_FOLDER);

	}

	public void testUnPhasedVcfFileHapiURPhase3() throws InterruptedException,
			IOException {

		FileUtil.createDirectory(TEMP_FOLDER);

		// chunk
		VcfChunk chunk = new VcfChunk();
		chunk.setChromosome("1");
		chunk.setStart(1);
		chunk.setEnd(10000000);
		chunk.setPhased(false);
		chunk.setVcfFilename("test-data/chr1.unphased.vcf");

		pipeline.setPattern(REF_PHASE3);
		pipeline.setPhasing("shapeit");

		VcfChunkOutput outputChunk = new VcfChunkOutput(chunk, TEMP_FOLDER);
		outputChunk.setVcfFilename(chunk.getVcfFilename());

		assertTrue(pipeline.execute(chunk, outputChunk));

		// simple phased check of hapi-ur output
		String content = FileUtil.readFileAsString(outputChunk
				.getPhasedVcfFilename());
		assertFalse(content.contains("/"));
		assertTrue(content.contains("|"));

		FileUtil.deleteDirectory(TEMP_FOLDER);

	}
}
