package genepi.minicloudmac.hadoop.imputation.sort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeKeyComparator extends WritableComparator {

	protected CompositeKeyComparator() {
		super(ChunkKey.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {

		ChunkKey ip1 = (ChunkKey) w1;
		ChunkKey ip2 = (ChunkKey) w2;

		if (ip1.chromosome.compareTo(ip2.chromosome) == 0) {

			if (ip1.sample.compareTo(ip2.sample) == 0) {

				if (ip1.startPosition == ip2.startPosition) {
					return 0;
				} else {
					if (ip1.startPosition > ip2.startPosition) {
						return 1;
					} else {
						return -1;
					}
				}

			} else {
				return ip1.sample.compareTo(ip2.sample);
			}

		} else {
			return ip1.chromosome.compareTo(ip2.chromosome);
		}

	}

}