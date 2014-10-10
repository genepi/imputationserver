package genepi.imputationserver.util;

import genepi.hadoop.HadoopJob;
import genepi.hadoop.common.WorkflowContext;
import genepi.hadoop.common.WorkflowStep;

import java.io.IOException;

import cloudgene.mapred.jobs.Message;

public abstract class HadoopJobStep extends WorkflowStep {

	private HadoopJob job;

	public boolean executeHadoopJob(HadoopJob job, WorkflowContext context) {

		this.job = job;
		context.beginTask("Running Hadoop Job...");
		boolean successful = job.execute();

		if (successful) {
			context.endTask("Execution successful.", Message.OK);
			return true;
		} else {

			// String logs = FileUtil.path(context.getLocalOutput(), "logs");
			// FileUtil.createDirectory(logs);

			// job.downloadFailedLogs(logs);

			context.endTask("Execution failed. Please have a look at the logfile for details.",
					Message.ERROR);

			return false;
		}
	}

	@Override
	public void kill() {
		try {
			job.kill();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
