package genepi.minicloudmac.hadoop.imputation;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cloudgene.mapred.jobs.CloudgeneContext;
import cloudgene.mapred.jobs.CloudgeneStep;
import cloudgene.mapred.steps.Hadoop;
import cloudgene.mapred.util.FileUtil;
import cloudgene.mapred.util.HadoopUtil;
import cloudgene.mapred.util.Settings;

public abstract class ParallelHadoopStep extends CloudgeneStep {

	private Map<String, String> jobIds;

	private BlockingQueue<Runnable> queueThreadPool;

	private int THREADS = 22;

	private ThreadPoolExecutor threadPool;

	protected static final Log log = LogFactory.getLog(Hadoop.class);

	public ParallelHadoopStep(int threads) {
		queueThreadPool = new ArrayBlockingQueue<Runnable>(100);

		threadPool = new ThreadPoolExecutor(THREADS, THREADS, 10,
				TimeUnit.SECONDS, queueThreadPool);

		jobIds = new HashMap<String, String>();
	}

	protected void waitForAll() throws InterruptedException {

		while (threadPool.getActiveCount() > 0) {
			Thread.sleep(500);
		}
		threadPool.shutdown();

	}

	protected void executeJarInBackground(String id, CloudgeneContext context,
			String jar, String... params) {
		HadoopBackgroundJob job = new HadoopBackgroundJob(id);
		job.setContext(context);
		job.setJar(jar);
		job.setParams(params);

		Future<?> future = threadPool.submit(job);

	}

	class HadoopBackgroundJob implements Runnable {

		private CloudgeneContext context;

		private String jar;

		private String[] params;

		private String id;

		public HadoopBackgroundJob(String id) {
			this.id = id;
		}

		public void setContext(CloudgeneContext context) {
			this.context = context;
		}

		public String getJar() {
			return jar;
		}

		public void setJar(String jar) {
			this.jar = jar;
		}

		public String[] getParams() {
			return params;
		}

		public void setParams(String[] params) {
			this.params = params;
		}

		public CloudgeneContext getContext() {
			return context;
		}

		@Override
		public void run() {

			try {

				onJobStart(id);

				boolean result = executeJar(context, jar, params);

				if (!result) {
					onJobFinish(id, false);
				} else {
					onJobFinish(id, true);
				}

			} catch (IOException e) {
				// e.printStackTrace();
				onJobFinish(id, false);
			} catch (InterruptedException e) {
				// e.printStackTrace();
				onJobFinish(id, false);
			}

		}

		protected boolean executeJar(CloudgeneContext context, String jar,
				String... params) throws IOException, InterruptedException {

			String hadoopPath = Settings.getInstance().getHadoopPath();
			String hadoop = FileUtil.path(hadoopPath, "bin", "hadoop");

			// hadoop jar or streaming
			List<String> command = new Vector<String>();

			command.add(hadoop);
			command.add("jar");
			command.add(jar);
			for (String param : params) {
				command.add(param.trim());
			}
			return executeCommand(command, context);
		}

		protected boolean executeCommand(List<String> command,
				CloudgeneContext context) throws IOException,
				InterruptedException {
			// set global variables
			for (int j = 0; j < command.size(); j++) {

				String cmd = command.get(j).replaceAll("\\$job_id",
						context.getJob().getId());
				command.set(j, cmd);
			}

			log.info(command);

			context.println("Command: " + command);
			context.println("Working Directory: "
					+ new File(context.getWorkingDirectory()).getAbsolutePath());

			ProcessBuilder builder = new ProcessBuilder(command);
			builder.directory(new File(context.getWorkingDirectory()));
			builder.redirectErrorStream(true);
			Process process = builder.start();
			InputStream is = process.getInputStream();
			InputStreamReader isr = new InputStreamReader(is);
			BufferedReader br = new BufferedReader(isr);
			String line = null;

			// Find job id and write output into file
			Pattern pattern = Pattern.compile("Running job: (.*)");
			Pattern pattern2 = Pattern.compile("HadoopJobId: (.*)");

			context.println("Output: ");
			while ((line = br.readLine()) != null) {

				Matcher matcher = pattern.matcher(line);
				if (matcher.find()) {
					// write statistics from old job
					String jobId = matcher.group(1).trim();
					log.info("Job " + context.getJob().getId()
							+ " -> HadoopJob " + jobId);
					jobIds.put(id, jobId);

				} else {
					Matcher matcher2 = pattern2.matcher(line);
					if (matcher2.find()) {
						String jobId = matcher2.group(1).trim();
						log.info("Job " + context.getJob().getId()
								+ " -> HadoopJob " + jobId);
						jobIds.put(id, jobId);
					}
				}

				context.println("  " + line);
			}

			br.close();
			isr.close();
			is.close();

			process.waitFor();
			context.println("Exit Code: " + process.exitValue());
			if (process.exitValue() != 0) {
				return false;
			} else {
				process.destroy();
			}
			return true;
		}

	}

	protected synchronized void onJobFinish(String id, boolean successful) {

	}

	protected synchronized void onJobStart(String id) {

	}

	@Override
	public void kill() {

		threadPool.purge();

		try {

			for (String jobId : jobIds.values()) {

				if (jobId != null) {

					log.info(" Cancel Job " + jobId);

					HadoopUtil.getInstance().kill(jobId);

				}

			}

		} catch (IOException e) {

			log.error(" Cancel Job failed: ", e);

		}

		threadPool.shutdownNow();

	}
	
	public String getHadoopJobId(String id){
		return jobIds.get(id);
	}

}
