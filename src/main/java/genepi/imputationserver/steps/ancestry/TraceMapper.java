package genepi.imputationserver.steps.ancestry;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.gson.Gson;

import genepi.hadoop.HdfsUtil;

public class TraceMapper extends Mapper<LongWritable, Text, NullWritable, NullWritable> {

	public static String REFERENCE_SITE = "reference.site";

	public static String REFERENCE_RANGE = "reference.range";

	public static String REFERENCE_GENO = "reference.geno";

	public static String REFERENCE_PC_COORD = "reference.RefPC.coord";

	public static String REFERENCE_SAMPLES = "reference.samples";

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		HdfsUtil.setDefaultConfiguration(context.getConfiguration());

		try {

			Gson gson = new Gson();
			TraceBatch batch = gson.fromJson(value.toString(), TraceBatch.class);

			TracePipeline pipeline = new TracePipeline();
			pipeline.setReferenceSite(REFERENCE_SITE);
			pipeline.setReferenceRange(REFERENCE_RANGE);
			pipeline.setReferenceGeno(REFERENCE_GENO);
			pipeline.setReferencePCCoord(REFERENCE_PC_COORD);
			pipeline.setReferenceSamples(REFERENCE_SAMPLES);
			pipeline.execute(batch);

			HdfsUtil.put(pipeline.getProPCCoord(),
					batch.getOutputStudyPc() + "/batch-" + batch.getBatch() + ".ProPC.coord");
			HdfsUtil.put(pipeline.getPopulations(),
					batch.getOutputStudyPopulation() + "/batch-" + batch.getBatch() + ".txt");

		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException(e);
		} catch (Error e) {
			e.printStackTrace();
			throw new IOException(e);
		}
	}

}
