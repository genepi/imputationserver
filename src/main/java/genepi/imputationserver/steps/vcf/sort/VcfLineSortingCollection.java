package genepi.imputationserver.steps.vcf.sort;

import java.io.File;

import htsjdk.samtools.util.SortingCollection;

public class VcfLineSortingCollection {

	public static SortingCollection<VcfLine> newInstance(int maxRecords, String tempDir) {
		return SortingCollection.newInstance(VcfLine.class, new VcfLineCodec(), new VcfLineComparator(), maxRecords,
				new File(tempDir));
	}

}
