package genepi.minicloudmac.hadoop.imputation.sort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class NaturalKeyGroupingComparator extends WritableComparator {

	protected NaturalKeyGroupingComparator() {
		super(ChunkKey.class, true);
	}

	@Override
	public int compare(WritableComparable o1, WritableComparable o2) {

		ChunkKey tsK1 = (ChunkKey) o1;
		ChunkKey tsK2 = (ChunkKey) o2;

		return tsK1.getChromosome().compareTo(tsK2.getChromosome());

	}

}