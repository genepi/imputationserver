package steps.imputation;

import static org.junit.Assert.*;
import genepi.hadoop.command.Command;
import genepi.imputationserver.steps.imputationMinimac3.ImputationPipelineMinimac3;
import genepi.imputationserver.steps.vcf.VcfChunk;
import genepi.imputationserver.steps.vcf.VcfChunkOutput;
import genepi.io.FileUtil;
import genepi.io.text.LineReader;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Vector;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class PipelineTest {

	@Parameters(name = "Ref: {0} - Phasing: {1}")
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][] {
				/*{ PipelineFactory.REF_PHASE1, "shapeit" },*/
				{ PipelineFactory.REF_PHASE1, "hapiur" },
				/*{ PipelineFactory.REF_PHASE3, "shapeit" }/*,
				{ PipelineFactory.REF_PHASE3, "hapiur" } */});
	}

	private String referencePanel;

	private String phasing;
	
	private String phenotypes = "test-data/gckd/phenotypes.txt";

	public PipelineTest(String referencePanel, String phasing) {
		this.referencePanel = referencePanel;
		this.phasing = phasing;
	}

	@Test
	public void testPipeline() throws InterruptedException, IOException {

		String tempFolder = "temp-" + referencePanel
				+ (phasing != null ? "-" + phasing : "");

		FileUtil.deleteDirectory(tempFolder);
		FileUtil.createDirectory(tempFolder);

		// chunk
		VcfChunk chunk = ChunkFactory.getChunkForGckdTestSnp();
		if (phasing == null) {
			chunk.setPhased(true);
		} else {
			chunk.setPhased(false);
		}

		ImputationPipelineMinimac3 pipeline = PipelineFactory
				.createPipelineByReference(referencePanel);
		if (phasing != null) {
			pipeline.setPhasing(phasing);
		}

		VcfChunkOutput outputChunk = new VcfChunkOutput(chunk, tempFolder);
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

		//run snptest to check control snp		
		/*Command snptest =new Command(FileUtil.path(PipelineFactory.BINARIES, "snptest_v2.5.2"));
		List<String> params = new Vector<String>();
		params.add("-data");
		params.add(outputChunk.getImputedVcfFilename());
		params.add(phenotypes);
		params.add("-o");
		params.add("output/gwas-results.txt");
		params.add("-frequentist");
		params.add("1");
		params.add("-method");
		params.add("expected");
		params.add("-pheno");
		params.add("CYS");
		params.add("-genotype_field GP");
		params.add("-range");
		params.add("20:23512737-23712737");
		params.add("-cov_all");
		params.add("-use_raw_phenotypes");		
		snptest.setParams(params);
		int result = snptest.execute();*/
		
		//assertEquals(0, result);
		
	}

}
