package genepi.imputationserver.steps.qc;

import genepi.hadoop.HdfsUtil;
import genepi.hadoop.io.HdfsLineWriter;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class QualityControlReducer extends Reducer<Text, Text, Text, Text> {

	private String output;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {

		output = context.getConfiguration().get(QualityControlJob.OUTPUT_MANIFEST);

	}

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		String filename = HdfsUtil.path(output, key.toString());
		HdfsLineWriter writer = new HdfsLineWriter(filename);
		for (Text value : values) {
			writer.write(value.toString());
		}
		writer.close();

	}

}
