package steps.imputation;

import genepi.imputationserver.steps.imputationMinimac3.ImputationPipelineMinimac3;
import genepi.io.FileUtil;

public class PipelineFactory {

	public static final String BINARIES = "files/minimac/bin";
	
	public static final int DEFAULT_PHASING_WINDOW = 250000;
	//public static final int DEFAULT_PHASING_WINDOW = 1000000;
	public static final int DEFAULT_MINIMAC_WINDOW = 50000;
	public static final int DEFAULT_ROUNDS = 0;
	public static final String TEMP_FOLDER = "temp";

	public static final String REF_PHASE1 = "$chr.1000g.Phase1.v3.With.Parameter.Estimates.m3vcf.gz";
	public static final String REF_PHASE3 = "$chr.1000g.Phase3.v5.With.Parameter.Estimates.m3vcf.gz";

	
	public static ImputationPipelineMinimac3 createPipelineByReference(String reference){
			

			// config pipeline
			ImputationPipelineMinimac3 pipeline = new ImputationPipelineMinimac3();
			pipeline.setMinimacCommand(FileUtil.path(BINARIES, "Minimac3"));
			pipeline.setHapiUrCommand(FileUtil.path(BINARIES, "hapi-ur"));
			pipeline.setVcfCookerCommand(FileUtil.path(BINARIES, "vcfCooker"));
			pipeline.setVcf2HapCommand(FileUtil.path(BINARIES, "vcf2hap"));
			pipeline.setShapeItCommand(FileUtil.path(BINARIES, "shapeit"));
			pipeline.setHapiUrPreprocessCommand(FileUtil.path(BINARIES,
					"insert-map.pl"));
			pipeline.setEagleCommand(FileUtil.path(BINARIES, "eagle_r373"));


			String refFilename = "test-data/reference-panels";

			String mapShapeITPattern = "genetic_map_chr$chr_combined_b37.txt";
			String mapShapeITFilename = refFilename;

			String mapHapiURPattern = "genetic_map_chr$chr_combined_hapiur_b37.txt";
			String mapHapiURFilename = refFilename;

			String mapEagleFilename = FileUtil.path(refFilename, "genetic_map_hg19.txt.gz");
			String refEagleFilename = refFilename;
			String refEaglePattern = "HRC.r1-1.GRCh37.chr$chr.shapeit3.mac5.aa.genotypes.bcf";
			
			pipeline.setPhasingWindow(DEFAULT_PHASING_WINDOW);
			pipeline.setRounds(DEFAULT_ROUNDS);
			pipeline.setMinimacWindow(DEFAULT_MINIMAC_WINDOW);
			pipeline.setRefFilename(refFilename);
			pipeline.setPattern(reference);
			pipeline.setMapShapeITPattern(mapShapeITPattern);
			pipeline.setMapShapeITFilename(mapShapeITFilename);
			pipeline.setMapHapiURFilename(mapHapiURFilename);
			pipeline.setMapHapiURPattern(mapHapiURPattern);

			pipeline.setMapShapeITPattern(mapShapeITPattern);
			pipeline.setMapShapeITFilename(mapShapeITFilename);
			pipeline.setMapHapiURFilename(mapHapiURFilename);
			pipeline.setMapHapiURPattern(mapHapiURPattern);
			
			pipeline.setMapEagleFilename(mapEagleFilename);
			pipeline.setRefEagleFilename(refEagleFilename);
			pipeline.setRefEaglePattern(refEaglePattern);
			
			pipeline.setPopulation("EUR");
			pipeline.setPhasing("shapeit");

			
			FileUtil.deleteDirectory(TEMP_FOLDER);

			FileUtil.createDirectory(TEMP_FOLDER);

			return pipeline;
		
	}
	
}
