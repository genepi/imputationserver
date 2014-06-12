package genepi.imputationserver.util;

import genepi.hadoop.HadoopJob;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cloudgene.mapred.jobs.CloudgeneContext;
import cloudgene.mapred.jobs.CloudgeneStep;
import cloudgene.mapred.steps.Hadoop;

public abstract class ParallelHadoopJobStep extends CloudgeneStep {

	private Map<String, HadoopJob> jobs;

	private BlockingQueue<Runnable> queueThreadPool;

	private int THREADS = 22;

	private ThreadPoolExecutor threadPool;

	protected static final Log log = LogFactory.getLog(Hadoop.class);

	public ParallelHadoopJobStep(int threads) {
		queueThreadPool = new ArrayBlockingQueue<Runnable>(100);

		threadPool = new ThreadPoolExecutor(THREADS, THREADS, 10,
				TimeUnit.SECONDS, queueThreadPool);

		jobs = new HashMap<String, HadoopJob>();
	}

	protected void waitForAll() throws InterruptedException {

		while (threadPool.getActiveCount() > 0) {
			Thread.sleep(500);
		}
		threadPool.shutdown();

	}

	protected void executeJarInBackground(String id, CloudgeneContext context,
			HadoopJob hadoopJob) {
		BackgroundHadoopJob job = new BackgroundHadoopJob(id, hadoopJob);
		job.setContext(context);
		jobs.put(id, hadoopJob);
		Future<?> future = threadPool.submit(job);

	}

	class BackgroundHadoopJob implements Runnable {

		private CloudgeneContext context;

		private String id;

		private HadoopJob job;

		public BackgroundHadoopJob(String id, HadoopJob job) {
			this.id = id;
			this.job = job;
		}

		public void setContext(CloudgeneContext context) {
			this.context = context;
		}

		public CloudgeneContext getContext() {
			return context;
		}

		@Override
		public void run() {

			onJobStart(id);
			log.info("Running job " + id + "....");
			boolean successful = job.execute();
			log.info("job " + id + " finished. sucessful " + successful);
			onJobFinish(id, successful);

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

			for (HadoopJob job : jobs.values()) {

				if (job != null) {

					log.info(" Cancel Job " + job.getJobId());
					job.kill();

				}

			}

		} catch (IOException e) {

			log.error(" Cancel Job failed: ", e);

		}

		threadPool.shutdownNow();

	}

	public HadoopJob getHadoopJob(String id) {
		return jobs.get(id);
	}

}
