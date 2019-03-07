package genepi.imputationserver;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import genepi.imputationserver.steps.FastQualityControlTest;
import genepi.imputationserver.steps.ImputationTest;
import genepi.imputationserver.steps.ImputationChrXTest;
import genepi.imputationserver.steps.ImputationPipelineTest;
import genepi.imputationserver.steps.InputValidationTest;
import genepi.imputationserver.steps.fastqc.VCFLineParserTest;
import genepi.imputationserver.steps.util.FileMergerTest;

@RunWith(Suite.class)
@SuiteClasses({ InputValidationTest.class, VCFLineParserTest.class, ImputationTest.class, FileMergerTest.class,
	FastQualityControlTest.class, ImputationPipelineTest.class, ImputationChrXTest.class})
public class AllTests {

}
