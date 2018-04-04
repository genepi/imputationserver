package genepi.imputationserver;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import genepi.imputationserver.steps.FastQualityControlTest;

@RunWith(Suite.class)
@SuiteClasses({ FastQualityControlTest.class})
public class QcTestSuite {

}
