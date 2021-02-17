package genepi.imputationserver.steps.fastqc;

import java.io.File;
import java.io.IOException;

import com.google.common.io.Files;

import genepi.imputationserver.steps.vcf.VcfChunk;
import genepi.imputationserver.util.RefPanel;
import genepi.io.text.LineWriter;
import junit.framework.TestCase;

public class StatisticsTaskTest extends TestCase {

	public void testSingleFile() throws IOException, InterruptedException {

		File myTempDir = Files.createTempDir();
		System.out.println("Temp Directory: " + myTempDir.getAbsolutePath());

		LineWriter excludedSnpsWriter = new LineWriter(myTempDir.getAbsolutePath() + "/excluded-snps.txt");
		excludedSnpsWriter.write("#Position" + "\t" + "FilterType" + "\t" + " Info", false);

		StatisticsTask task = new StatisticsTask();
		task.setVcfFilenames("test-data/data/single/minimac_test.50.vcf.gz");
		task.setChunkFileDir(myTempDir.getAbsolutePath());
		task.setChunksDir(myTempDir.getAbsolutePath());
		task.setBuild("hg19");
		task.setChunkSize(20000000);
		task.setPhasingWindow(5000000);
		task.setExcludedSnpsWriter(excludedSnpsWriter);
		task.setLegendFile("test-data/configs/hapmap-chr1/ref-panels/hapmap_r22.chr$chr.CEU.hg19_impute.legend.gz");
		task.setMafFile(myTempDir.getAbsolutePath() + "/maf-file.txt");
		task.setMinSnps(Integer.parseInt(RefPanel.MIN_SNPS));
		task.setMixedGenotypesChrX(Double.parseDouble(RefPanel.CHR_X_MIXED_GENOTYPES));
		task.setPopulation("mixed");
		task.setReferenceOverlap(Double.parseDouble(RefPanel.OVERLAP));
		task.setRefSamples(60);
		task.setMinSampleCallRate(Double.parseDouble(RefPanel.SAMPLE_CALL_RATE));
		task.setStatDir(myTempDir.getAbsolutePath());
		task.setAlleleFrequencyCheck(true);

		TaskResult result = task.run(new TaskProgressListenerMock());
		assertTrue(result.isSuccess());

		excludedSnpsWriter.close();

		assertEquals(336, task.getFiltered());
		assertEquals(96, task.getOverallSnps());
		assertEquals(185, task.getAlternativeAlleles());
		assertEquals(331, task.getMonomorphic());

		assertEquals(1, task.getChunks().size());
		VcfChunk chunk = task.getChunks().get(0);
		assertEquals(96, chunk.overallSnpsChunk);

	}

	class TaskProgressListenerMock implements ITaskProgressListener {
		@Override
		public void progress(String message) {
			System.out.println(message);
		}
	}

}
