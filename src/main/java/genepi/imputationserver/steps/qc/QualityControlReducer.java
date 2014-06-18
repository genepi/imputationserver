package genepi.imputationserver.steps.qc;

import genepi.hadoop.HdfsUtil;
import genepi.hadoop.io.HdfsLineWriter;
import genepi.imputationserver.steps.vcf.VcfChunk;
import genepi.imputationserver.steps.vcf.VcfChunkUtil;

import java.io.IOException;
import java.util.List;
import java.util.Vector;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class QualityControlReducer extends Reducer<Text, Text, Text, Text> {

	private String output;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {

		output = context.getConfiguration().get(
				QualityControlJob.OUTPUT_MANIFEST);

	}

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		// load chunks
		List<VcfChunk> chunks = new Vector<VcfChunk>();
		for (Text value : values) {
			VcfChunk chunk = new VcfChunk(value.toString());
			chunks.add(chunk);
		}

		// merge excluded chunks
		//VcfChunkUtil.mergeExcludedChunks(chunks);

		// write merged chunks to manifest file
		String filename = HdfsUtil.path(output, key.toString());
		HdfsLineWriter writer = new HdfsLineWriter(filename);
		for (VcfChunk chunk : chunks) {
			writer.write(chunk.serialize());
		}
		writer.close();

	}

}
