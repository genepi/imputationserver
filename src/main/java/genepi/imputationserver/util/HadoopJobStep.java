package genepi.imputationserver.util;

import java.io.IOException;

import cloudgene.sdk.internal.WorkflowContext;
import cloudgene.sdk.internal.WorkflowStep;
import genepi.hadoop.HadoopJob;


public abstract class HadoopJobStep extends WorkflowStep {

	private HadoopJob job;

	public boolean executeHadoopJob(HadoopJob job, WorkflowContext context) {

		this.job = job;
		context.beginTask("Running Hadoop Job...");
		boolean successful = job.execute();

		if (successful) {
			context.endTask("Execution successful.", WorkflowContext.OK);
			return true;
		} else {

			// String logs = FileUtil.path(context.getLocalOutput(), "logs");
			// FileUtil.createDirectory(logs);

			// job.downloadFailedLogs(logs);

			context.endTask("Execution failed. Please have a look at the logfile for details.",
					WorkflowContext.ERROR);

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
