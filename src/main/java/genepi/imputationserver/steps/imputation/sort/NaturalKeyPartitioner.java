package genepi.minicloudmac.hadoop.imputation.sort;

import org.apache.hadoop.mapreduce.Partitioner;

public class NaturalKeyPartitioner extends Partitioner<ChunkKey, ChunkValue> {

	@Override
	public int getPartition(ChunkKey key, ChunkValue value, int numPartitions) {
		return Math.abs(key.getChromosome().hashCode() * 127) % numPartitions;
	}

}