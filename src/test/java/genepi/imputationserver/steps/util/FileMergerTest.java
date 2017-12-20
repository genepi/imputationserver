package genepi.imputationserver.steps.util;

import genepi.imputationserver.util.FileMerger;
import junit.framework.TestCase;

public class FileMergerTest extends TestCase {

	public void testKeepVcfLineByInfo() {
		assertTrue(FileMerger.keepVcfLineByInfo("lukas=3223;test=22;R2=0.8", "R2", 0.5f));
		assertFalse(FileMerger.keepVcfLineByInfo("lukas=3223;test=22;R2=0.2", "R2", 0.5f));
		assertTrue(FileMerger.keepVcfLineByInfo("lukas=3223;test=22;R2=0.8;ttt=dsa", "R2", 0.5f));
		assertFalse(FileMerger.keepVcfLineByInfo("lukas=3223;test=22;R2=0.2;rrr=eeew", "R2", 0.5f));
		assertTrue(FileMerger.keepVcfLineByInfo("R2=0.8;ttt=dsa", "R2", 0.5f));
		assertFalse(FileMerger.keepVcfLineByInfo("R2=0.2;rrr=eeew", "R2", 0.5f));
		assertTrue(FileMerger.keepVcfLineByInfo("rrr=eeew", "R2", 0.5f));
	}

	public void testParseInfo() {

		String line = "20\t14370\trs6054257\tG\tA\t29\tPASS\tNS=3;DP=14;AF=0.5;DB;H2\tGT:GQ:DP:HQ 0|0:48:1:51,51 1|0:48:8:51,51";
		String info = FileMerger.parseInfo(line);
		assertEquals("NS=3;DP=14;AF=0.5;DB;H2", info);

		// wong format
		line = "20\t14370\trs6054257\tG\tA\t29";
		info = FileMerger.parseInfo(line);
		assertEquals(null, info);

	}

}
