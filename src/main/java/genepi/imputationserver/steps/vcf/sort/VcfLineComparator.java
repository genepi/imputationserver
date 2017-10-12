package genepi.imputationserver.steps.vcf.sort;

import java.util.Comparator;

public class VcfLineComparator implements Comparator<VcfLine> {

	@Override
	public int compare(VcfLine o1, VcfLine o2) {
		return Long.compare(o1.getPosition(), o2.getPosition());
	}

}
