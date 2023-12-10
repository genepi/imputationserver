package genepi.imputationserver.steps.ancestry;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import com.google.gson.Gson;

import cloudgene.sdk.internal.WorkflowContext;
import cloudgene.sdk.internal.WorkflowStep;
import genepi.hadoop.HdfsUtil;
import genepi.imputationserver.steps.FastQualityControl;
import genepi.imputationserver.steps.ancestry.TraceInputValidation.TraceInputValidationResult;
import genepi.imputationserver.steps.fastqc.ITask;
import genepi.imputationserver.steps.fastqc.ITaskProgressListener;
import genepi.imputationserver.steps.fastqc.LiftOverTask;
import genepi.imputationserver.steps.fastqc.TaskResults;
import genepi.imputationserver.steps.vcf.VcfFile;
import genepi.imputationserver.steps.vcf.VcfFileUtil;
import genepi.imputationserver.util.DefaultPreferenceStore;
import genepi.io.FileUtil;

public class TraceStep extends WorkflowStep {

	public static String BUILD = "hg19";

	public static final int MIN_VARIANTS = 100;

	@Override
	public boolean run(WorkflowContext context) {
		if (prepareTraceJobs(context)) {
			estimateAncestries(context);
			return true;
		} else {
			return true;
		}
	}

	protected void setupTabix(String folder) {
		VcfFileUtil.setTabixBinary(FileUtil.path(folder, "bin", "tabix"));
	}

	public boolean prepareTraceJobs(WorkflowContext context) {

		String folder = getFolder(FastQualityControl.class);
		setupTabix(folder);

		String genotypes = context.get("files");
		String buildGwas = context.get("build");

		int batchSize = Integer.parseInt(context.get("batch_size"));
		String output = context.get("trace_batches");
		String vcfHdfsDir = HdfsUtil.path(context.getHdfsTemp(), "genotypes");

		String[] files = FileUtil.getFiles(genotypes, "*.vcf.gz$|*.vcf$");

		try {

			// load job.config
			File jobConfig = new File(FileUtil.path(folder, "job.config"));
			DefaultPreferenceStore store = new DefaultPreferenceStore();
			if (jobConfig.exists()) {
				store.load(jobConfig);
			} else {
				context.log(
						"Configuration file '" + jobConfig.getAbsolutePath() + "' not available. Use default values.");
			}

			// Check if liftover is needed
			if (!buildGwas.equals(BUILD)) {

				context.warning("Uploaded data is " + buildGwas + " and reference is " + BUILD + ".");
				String chainFile = store.getString(buildGwas + "To" + BUILD);
				if (chainFile == null) {
					context.error("Currently we do not support liftOver from " + buildGwas + " to " + BUILD);
					return false;
				}

				String fullPathChainFile = FileUtil.path(folder, chainFile);
				if (!new File(fullPathChainFile).exists()) {
					context.error("Chain file " + fullPathChainFile + " not found.");
					return false;
				}

				String chunksDir = FileUtil.path(context.getLocalTemp(), "liftover");
				FileUtil.createDirectory(chunksDir);

				LiftOverTask task = new LiftOverTask();
				task.setVcfFilenames(files);
				task.setChainFile(fullPathChainFile);
				task.setChunksDir(chunksDir);
				task.setExcludedSnpsWriter(null);

				TaskResults results = runTask(context, task);

				if (results.isSuccess()) {
					files = task.getNewVcfFilenames();
				} else {
					return false;
				}

			}

			String mergedFile = FileUtil.path(context.getLocalTemp(), "study.merged.vcf.gz");

			if (!checkDataAndMerge(context, files, mergedFile)) {
				return false;
			}

			context.beginTask("Preparing TRACE jobs");

			FileSystem hdfs = FileSystem.get(HdfsUtil.getConfiguration());

			context.log("Put file " + mergedFile);
			HdfsUtil.put(mergedFile, HdfsUtil.path(vcfHdfsDir, "study.merged.vcf.gz"));

			// read number of samples from first vcf file
			VcfFile vcfFile = VcfFileUtil.load(mergedFile, 200000, false);

			int nIndividuals = vcfFile.getNoSamples();
			int batch = 0;
			int start = 1;
			int end;

			while (start <= nIndividuals) {
				end = start + batchSize - 1;
				if (end > nIndividuals) {
					end = nIndividuals;
				}

				TraceBatch traceBatch = new TraceBatch();
				traceBatch.setBatch(String.format("%05d", batch));
				traceBatch.setStart(start);
				traceBatch.setEnd(end);
				traceBatch.setStudyVcf(vcfHdfsDir);
				traceBatch.setDim(Integer.parseInt(context.get("dim")));
				traceBatch.setDimHigh(Integer.parseInt(context.get("dim_high")));
				traceBatch.setOutputStudyPc(context.get("study_pc"));
				traceBatch.setOutputStudyPopulation(context.get("study_population"));

				Gson gson = new Gson();
				BufferedWriter writer = new BufferedWriter(
						new OutputStreamWriter(hdfs.create(new Path(output, batch + ".batch"))));
				writer.write(gson.toJson(traceBatch));
				writer.close();

				start = end + 1;
				batch++;

				context.log("\t- Created batch No. " + batch);
			}

			context.endTask("Prepared " + batch + " batch job" + ((batch > 1) ? "s." : "."), WorkflowContext.OK);

			return true;

		} catch (IOException e) {

			context.error("An internal server error occurred.\n" + exceptionToString(e));

		}

		context.error("Execution failed. Please, contact administrator.");

		return false;
	}

	public boolean checkDataAndMerge(WorkflowContext context, String[] files, String mergedFile) {

		try {

			context.beginTask("Input Validation");

			String reference = context.get("reference");
			String referencesHdfsDir = context.getConfig("references");

			String referenceSites = FileUtil.path(context.getLocalTemp(), "reference.site");
			HdfsUtil.get(referencesHdfsDir + "/" + reference + ".site", referenceSites);

			TraceInputValidation validation = new TraceInputValidation();

			TraceInputValidationResult result = validation.mergeAndCheckSites(files, referenceSites, mergedFile);

			String message = "Loaded " + result.getTotal() + " variants" + "\n" + "Variants with different alleles: "
					+ result.getAlleleMissmatch() + "\n" + "Variants with allele switches: "
					+ result.getAlleleSwitches() + "\n" + "Variants not found in reference: " + result.getNotFound()
					+ "\n" + "Overlapping variants used by LASER: " + result.getFound();

			context.endTask(message, WorkflowContext.OK);

			if (result.getFound() <= MIN_VARIANTS) {
				context.error("Number of variants shared with reference is too small (&le;" + MIN_VARIANTS
						+ ").\nPlease, check if input data are correct or try to use another ancestry reference panel.");
				return false;
			}

			return true;

		} catch (IOException e) {
			context.error("Input Validation failed:\n" + exceptionToString(e));
			return false;
		}
	}

	public boolean estimateAncestries(WorkflowContext context) {

		String reference = context.get("reference");
		String batchesDir = context.get("trace_batches");
		String localOutput = context.get("pgs_output");

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

			context.beginTask("Estimate ancestries...");

			context.log("\t- Running MapReduce jobs...");
			if (!job.waitForCompletion(true)) {
				context.error("Error while executing MapReduce job.");
				return false;
			}

			String outputSamples = FileUtil.path(localOutput, "samples.txt");
			HdfsUtil.merge(outputSamples, studyPopulation, true);
			String outputPC = FileUtil.path(localOutput, "samples.pca.txt");
			HdfsUtil.merge(outputPC, studyPC, true);

			context.endTask("Ancestries estimated.", WorkflowContext.OK);

			return true;

		} catch (IOException e) {
			context.error("An internal server error occurred while launching Hadoop job.\n" + exceptionToString(e));
		}

		context.error("Execution failed. Please, contact administrator.");
		return false;
	}

	protected TaskResults runTask(final WorkflowContext context, ITask task) {
		context.beginTask("Running " + task.getName() + "...");
		TaskResults results;
		try {
			results = task.run(new ITaskProgressListener() {

				@Override
				public void progress(String message) {
					context.updateTask(message, WorkflowContext.RUNNING);

				}
			});

			if (results.isSuccess()) {
				context.endTask(task.getName(), WorkflowContext.OK);
			} else {
				context.endTask(task.getName() + "\n" + results.getMessage(), WorkflowContext.ERROR);
			}
			return results;
		} catch (InterruptedException e) {
			TaskResults result = new TaskResults();
			result.setSuccess(false);
			result.setMessage(e.getMessage());
			context.println("Task '" + task.getName() + "' failed.\nException:" + exceptionToString(e));
			context.endTask(e.getMessage(), WorkflowContext.ERROR);
			return result;
		} catch (Exception e) {
			TaskResults result = new TaskResults();
			result.setSuccess(false);
			result.setMessage(e.getMessage());
			context.println("Task '" + task.getName() + "' failed.\nException:" + exceptionToString(e));
			context.endTask(task.getName() + " failed.", WorkflowContext.ERROR);
			return result;
		} catch (Error e) {
			TaskResults result = new TaskResults();
			result.setSuccess(false);
			result.setMessage(e.getMessage());
			context.println("Task '" + task.getName() + "' failed.\nException:" + exceptionToString(e));
			context.endTask(task.getName() + " failed.", WorkflowContext.ERROR);
			return result;
		}

	}

	private static String exceptionToString(Exception e) {
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		e.printStackTrace(pw);
		return sw.toString();
	}

	private static String exceptionToString(Error e) {
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		e.printStackTrace(pw);
		return sw.toString();
	}

}

