package genepi.imputationserver.steps.ancestry;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import genepi.hadoop.HdfsUtil;
import genepi.hadoop.common.WorkflowContext;
import genepi.hadoop.common.WorkflowStep;
import genepi.io.FileUtil;

public class TraceStep extends WorkflowStep {

	@Override
	public boolean run(WorkflowContext context) {

		String reference = context.get("reference");
		String batchesDir = context.get("trace_batches");
		String localOutput = context.get("local");

		String studyPC = context.get("study_pc");
		String studyPopulation = context.get("study_population");

		String referencesHdfsDir = context.getConfig("references");
		String binariesHDFS = context.getConfig("binaries");

		try {
			Job job = Job.getInstance(HdfsUtil.getConfiguration());

			context.log("\t- Distributing referece panel...");
			DistributedCache.createSymlink(job.getConfiguration());
			DistributedCache.addCacheFile(
					new URI(referencesHdfsDir + "/" + reference + ".range" + "#" + TraceMapper.REFERENCE_RANGE),
					job.getConfiguration());
			DistributedCache.addCacheFile(new URI(
					referencesHdfsDir + "/" + reference + ".RefPC.coord" + "#" + TraceMapper.REFERENCE_PC_COORD),
					job.getConfiguration());
			DistributedCache.addCacheFile(
					new URI(referencesHdfsDir + "/" + reference + ".site" + "#" + TraceMapper.REFERENCE_SITE),
					job.getConfiguration());
			DistributedCache.addCacheFile(
					new URI(referencesHdfsDir + "/" + reference + ".geno" + "#" + TraceMapper.REFERENCE_GENO),
					job.getConfiguration());
			DistributedCache.addCacheFile(
					new URI(referencesHdfsDir + "/" + reference + ".samples" + "#" + TraceMapper.REFERENCE_SAMPLES),
					job.getConfiguration());

			DistributedCache.addCacheFile(new URI(binariesHDFS + "/tabix#tabix"), job.getConfiguration());
			DistributedCache.addCacheFile(new URI(binariesHDFS + "/vcf2geno#vcf2geno"), job.getConfiguration());
			DistributedCache.addCacheFile(new URI(binariesHDFS + "/trace#trace"), job.getConfiguration());

			context.log("\t- Configuring MapReduce jobs...");
			job.setJarByClass(TraceStep.class);
			job.setJobName(context.getJobId() + ": TRACE input check");

			TextInputFormat.addInputPath(job, new Path(batchesDir));

			job.setMapperClass(TraceMapper.class);
			job.setMapOutputKeyClass(NullWritable.class);
			job.setOutputFormatClass(NullOutputFormat.class);
			job.getConfiguration().setInt("mapred.map.max.attempts", 1);
			job.setNumReduceTasks(0);
			// from yaml from daniel
			// job.getConfiguration().setInt("mapred.max.split.size", 1);
			// job.getConfiguration().setInt("mapred.min.split.size", 1);
			// job.getConfiguration().setInt("mapreduce.input.fileinputformat.split.minsize",
			// 1);
			// job.getConfiguration().setInt("mapreduce.input.fileinputformat.split.maxsize",
			// 1);

			context.log("\t- Running MapReduce jobs...");
			if (!job.waitForCompletion(true)) {
				context.error("Error while executing MapReduce job.");
				return false;
			}

			String outputSamples = FileUtil.path(localOutput, "samples.txt");
			HdfsUtil.merge(outputSamples, studyPopulation, true);
			String outputPC = FileUtil.path(localOutput, "samples.pca.txt");
			HdfsUtil.merge(outputPC, studyPC, true);

			return true;

		} catch (IOException e) {
			context.error("An internal server error occured while launching Hadoop job.");
			e.printStackTrace();
		} catch (InterruptedException e) {
			context.error("An internal server error occured while launching Hadoop job.");
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			context.error("An internal server error occured while launching Hadoop job.");
			e.printStackTrace();
		} catch (URISyntaxException e) {
			context.error("An internal server error occured while launching Hadoop job.");
			e.printStackTrace();
		}

		context.error("Execution failed. Please, contact administrator.");
		return false;
	}

}
