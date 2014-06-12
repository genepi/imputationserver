package genepi.imputationserver.util;

import genepi.hadoop.HadoopJob;

import java.io.IOException;

import cloudgene.mapred.jobs.CloudgeneContext;
import cloudgene.mapred.jobs.CloudgeneStep;
import cloudgene.mapred.jobs.Message;

public abstract class HadoopJobStep extends CloudgeneStep {

	private HadoopJob job;

	public boolean executeHadoopJob(HadoopJob job, CloudgeneContext context) {

		this.job = job;
		beginTask("Running Hadoop Job...");
		boolean successful = job.execute();
		if (successful) {
			endTask("Execution successful.", Message.OK);
			return true;
		} else {
			endTask("Execution failed. Please have a look at the logfile for details.",
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
		}
	}

}
