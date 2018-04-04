package genepi.imputationserver;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import genepi.imputationserver.steps.FastQualityControlTest;
import genepi.imputationserver.steps.ImputationMinimac3Test;
import genepi.imputationserver.steps.ImputationPipelineMinimac3Test;
import genepi.imputationserver.steps.InputValidationTest;
import genepi.imputationserver.steps.fastqc.VCFLineParserTest;
import genepi.imputationserver.steps.util.FileMergerTest;

@RunWith(Suite.class)
@SuiteClasses({ InputValidationTest.class, VCFLineParserTest.class, ImputationMinimac3Test.class, FileMergerTest.class,
		FastQualityControlTest.class, ImputationPipelineMinimac3Test.class })

public class AllTestsNoChrX {

}
