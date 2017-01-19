package genepi.imputationserver;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import genepi.imputationserver.steps.ImputationMinimac3Test;
import genepi.imputationserver.steps.InputValidationTest;
import genepi.imputationserver.steps.QCStatisticsTest;

@RunWith(Suite.class)
@SuiteClasses({ InputValidationTest.class, QCStatisticsTest.class, ImputationMinimac3Test.class })
public class AllTests {

}
