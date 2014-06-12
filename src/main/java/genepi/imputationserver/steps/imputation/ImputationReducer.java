package genepi.imputationserver.steps.imputation;

import genepi.hadoop.HdfsUtil;
import genepi.imputationserver.steps.imputation.sort.ChunkKey;
import genepi.imputationserver.steps.imputation.sort.ChunkValue;

import java.io.IOException;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ImputationReducer extends Reducer<ChunkKey, ChunkValue, Text, Text> {

	Text outKey = new Text();

	Text outValue = new Text();

	public void reduce(ChunkKey key, Iterable<ChunkValue> values,
			Context context) throws IOException, InterruptedException {

		String chr = key.chromosome;

		String oldSample = null;

		String outputFolder = context.getConfiguration().get(ImputationJob.OUTPUT);

		String output = HdfsUtil.path(outputFolder, chr, "merged.dose.gz");
		try {
			FileSystem fileSystem = FileSystem.get(context.getConfiguration());
			FSDataOutputStream fstream = fileSystem.create(new Path(output));

			GZIPOutputStream gzip = new GZIPOutputStream(fstream);

			for (ChunkValue value : values) {

				if (oldSample == null) {
					// first sample
					oldSample = value.sample;
					gzip.write(oldSample.getBytes());
					gzip.write("\t".getBytes());
					gzip.write("DOSE".getBytes());
				}

				if (!oldSample.equals(value.sample)) {
					// next sample
					gzip.write("\n".getBytes());
					gzip.write(value.sample.getBytes());
					gzip.write("\t".getBytes());
					gzip.write("DOSE".getBytes());
					oldSample = value.sample;
				}

				gzip.write("\t".getBytes());
				gzip.write(value.genotypes.getBytes());

			}

			gzip.close();

			fstream.close();

		} catch (Exception e) {
			System.err.println("Error: " + e.getMessage());
		}

	}

}
