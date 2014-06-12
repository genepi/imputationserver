package genepi.minicloudmac.hadoop.imputation.sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class ChunkValue implements WritableComparable<ChunkValue> {

	public String sample;

	public String genotypes;

	@Override
	public void readFields(DataInput arg0) throws IOException {
		sample = arg0.readUTF();
		genotypes = Text.readString(arg0).toString();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeUTF(sample);
		Text.writeString(arg0, genotypes);
	}

	@Override
	public int compareTo(ChunkValue o) {
		if (sample.compareTo(o.sample) == 0) {

			if (genotypes.compareTo(o.genotypes) == 0) {

				return 0;

			} else {
				return genotypes.compareTo(o.genotypes);
			}

		} else {
			return sample.compareTo(o.sample);
		}
	}

}
