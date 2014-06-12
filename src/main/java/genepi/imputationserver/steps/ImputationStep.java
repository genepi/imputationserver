package genepi.imputationserver.steps;

import genepi.hadoop.HdfsUtil;
import genepi.io.FileUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapred.RunningJob;

import cloudgene.mapred.jobs.CloudgeneContext;
import cloudgene.mapred.jobs.Message;
import cloudgene.mapred.steps.ParallelHadoopStep;
import cloudgene.mapred.util.HadoopUtil;
import cloudgene.mapred.wdl.WdlStep;

public class ImputationStep extends ParallelHadoopStep {

	private Map<String, MyJob> jobs;

	int WAIT = 0;

	int RUNNING = 1;

	int OK = 2;

	int FAILED = 3;

	Message message = null;

	public ImputationStep() {
		super(10);
		jobs = new HashMap<String, MyJob>();
	}

	@Override
	public boolean run(WdlStep step, CloudgeneContext context) {

		String input = context.get("mafchunkfile");
		String output = context.get("outputimputation");
		String reference = context.get("refpanel");
		String local = context.get("local");
		String chunk = context.get("chunksize");
		String window = context.get("window");
		String log = context.get("logfile");
		String phasing = context.get("phasing");

		try {
			List<String> chunkFiles = HdfsUtil.getFiles(input);

			message = createLogMessage("", Message.OK);

			for (String chunkFile : chunkFiles) {

				String[] tiles = chunkFile.split("/");
				String chr = tiles[tiles.length - 1];

				MyJob myJob = new MyJob();
				myJob.id = chunkFile;
				myJob.chromosome = chr;
				myJob.state = WAIT;
				jobs.put(chunkFile, myJob);

				updateMessage();

			}

			for (String chunkFile : chunkFiles) {

				String[] tiles = chunkFile.split("/");
				String chr = tiles[tiles.length - 1];

				executeJarInBackground(chunkFile, context, "minimac-cloud.jar",
						"imputation", "--input", chunkFile, "--output",
						HdfsUtil.path(output, chr), "--reference", reference,
						"--local", local, "--chunk", chunk, "--window", window,
						"--phasing", phasing, "--log",
						FileUtil.path(log, "chr_" + chr + ".log"));
			}

			waitForAll();

			updateProgress();
			updateMessage();
			message.setType(Message.OK);
			// message.setMessage("Done!");

		} catch (IOException e1) {
			e1.printStackTrace();
			return false;
		} catch (InterruptedException e1) {
			e1.printStackTrace();

			message.setType(Message.ERROR);
			message.setMessage("Canceled by user.");

			return false;
		}

		return true;

	}

	private synchronized void updateMessage() {

		String text = "";

		int i = 1;

		for (MyJob job : jobs.values()) {

			if (job.state == OK) {

				text += "<span class=\"badge badge-success\" style=\"width: 40px\">Chr "
						+ job.chromosome + "</span>";

			}
			if (job.state == RUNNING) {

				text += "<span class=\"badge badge-info\" style=\"width: 40px\">Chr "
						+ job.chromosome + "</span>";

			}
			if (job.state == FAILED) {

				text += "<span class=\"badge badge-important\" style=\"width: 40px\">Chr "
						+ job.chromosome + "</span>";

			}
			if (job.state == WAIT) {

				text += "<span class=\"badge\" style=\"width: 40px\">Chr "
						+ job.chromosome + "</span>";

			}

			if (i % 6 == 0) {
				text += "<br>";
			}

			i++;

		}

		text += "<br>";
		text += "<br>";
		text += "<span class=\"badge\" style=\"width: 8px\">&nbsp;</span> Waiting<br>";
		text += "<span class=\"badge badge-info\" style=\"width: 8px\">&nbsp;</span> Running<br>";
		text += "<span class=\"badge badge-success\" style=\"width: 8px\">&nbsp;</span> Complete";

		if (message != null) {

			message.setMessage(text);

		}

	}

	@Override
	protected synchronized void onJobStart(String id) {
	}

	@Override
	protected synchronized void onJobFinish(String id, boolean successful) {

		if (!successful) {
			kill();
		}

	}

	class MyJob {

		private String chromosome;

		private int state = WAIT;

		private String id;

		public String getChromosome() {
			return chromosome;
		}

		public void setChromosome(String chromosome) {
			this.chromosome = chromosome;
		}

		public int getState() {
			return state;
		}

		public void setState(int state) {
			this.state = state;
		}

		public void setId(String id) {
			this.id = id;
		}

		public String getId() {
			return id;
		}

	}

	@Override
	public void updateProgress() {

		for (MyJob job : jobs.values()) {
			String id = job.getId();
			String hadoopJobId = getHadoopJobId(id);
			RunningJob hadoopJob = HadoopUtil.getInstance().getJob(hadoopJobId);
			try {

				if (hadoopJob != null) {

					if (hadoopJob.isComplete()) {

						if (hadoopJob.isSuccessful()) {
							job.state = OK;
						} else {
							job.state = FAILED;
						}

					} else {

						if (hadoopJob.getJobStatus().mapProgress() > 0) {

							job.state = RUNNING;

						} else {
							job.state = WAIT;
						}

					}

				} else {
					job.state = WAIT;
				}
			} catch (IOException e) {

				job.state = WAIT;

			}

		}

		updateMessage();

	}

}
