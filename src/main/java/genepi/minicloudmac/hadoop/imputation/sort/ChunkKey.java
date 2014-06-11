package genepi.minicloudmac.hadoop.imputation.sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class ChunkKey implements WritableComparable<ChunkKey> {
	public String chromosome;

	public String sample;

	public long startPosition;

	public String getChromosome() {
		return chromosome;
	}

	public void setChromosome(String chromosome) {
		this.chromosome = chromosome;
	}

	public String getSample() {
		return sample;
	}

	public void setSample(String sample) {
		this.sample = sample;
	}

	public long getStartPosition() {
		return startPosition;
	}

	public void setStartPosition(long startPosition) {
		this.startPosition = startPosition;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		chromosome = arg0.readUTF();
		sample = arg0.readUTF();
		startPosition = arg0.readLong();

	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeUTF(chromosome);
		arg0.writeUTF(sample);
		arg0.writeLong(startPosition);
	}

	@Override
	public int compareTo(ChunkKey o) {
		if (chromosome.compareTo(o.chromosome) == 0) {

			if (sample.compareTo(o.sample) == 0) {

				if (startPosition == o.startPosition) {
					return 0;
				} else {
					if (startPosition > o.startPosition) {
						return 1;
					} else {
						return -1;
					}
				}

			} else {
				return sample.compareTo(o.sample);
			}

		} else {
			return chromosome.compareTo(o.chromosome);
		}
	}

}
