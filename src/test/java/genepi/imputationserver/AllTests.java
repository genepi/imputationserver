package genepi.imputationserver;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import genepi.imputationserver.steps.FastQualityControlTest;
import genepi.imputationserver.steps.ImputationMinimac3Test;
import genepi.imputationserver.steps.InputValidationTest;

@RunWith(Suite.class)
@SuiteClasses({ InputValidationTest.class, FastQualityControlTest.class, ImputationMinimac3Test.class })
public class AllTests {

}
