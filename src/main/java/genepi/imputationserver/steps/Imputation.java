package genepi.imputationserver.steps;

import genepi.hadoop.HadoopJob;
import genepi.hadoop.HdfsUtil;
import genepi.imputationserver.steps.imputation.ImputationJob;
import genepi.imputationserver.util.ParallelHadoopJobStep;
import genepi.imputationserver.util.RefPanel;
import genepi.imputationserver.util.RefPanelList;
import genepi.io.FileUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cloudgene.mapred.jobs.CloudgeneContext;
import cloudgene.mapred.jobs.Message;
import cloudgene.mapred.wdl.WdlStep;

public class Imputation extends ParallelHadoopJobStep {

	Message message = null;

	Map<String, HadoopJob> jobs = null;

	boolean error = false;

	private String errorChr = "";

	public Imputation() {
		super(10);
		jobs = new HashMap<String, HadoopJob>();
	}

	@Override
	public boolean run(WdlStep step, CloudgeneContext context) {

		String folder = getFolder(Imputation.class);

		// inputs
		String input = context.get("mafchunkfile");
		String reference = context.get("refpanel");
		String phasing = context.get("phasing");
		boolean noCache = false;
		String minimacBin = "minimac";

		if (context.get("nocache") != null) {
			noCache = context.get("nocache").equals("yes");
		}

		if (context.get("minimacbin") != null) {
			minimacBin = context.get("minimacbin");
		}

		// outputs
		String output = context.get("outputimputation");
		String local = context.get("local");
		String log = context.get("logfile");

		if (!HdfsUtil.exists(input)) {
			error("No chunks passed the QC step.");
			return false;
		}

		// load reference panels

		RefPanelList panels = null;
		try {
			panels = RefPanelList.loadFromFile(FileUtil.path(folder,
					"panels.txt"));

		} catch (Exception e) {

			error("panels.txt not found.");
			return false;
		}

		// check reference panel

		RefPanel panel = panels.getById(reference);
		if (panel == null) {
			error("Reference Panel '" + reference + "' not found.");
			return false;
		}

		// execute one job per chromosome

		try {
			List<String> chunkFiles = HdfsUtil.getFiles(input);

			message = createLogMessage("", Message.OK);

			for (String chunkFile : chunkFiles) {

				String[] tiles = chunkFile.split("/");
				String chr = tiles[tiles.length - 1];

				ImputationJob job = new ImputationJob("");
				job.setFolder(folder);
				job.setRefPanelHdfs(panel.getHdfs());
				job.setRefPanelPattern(panel.getPattern());
				job.setInput(chunkFile);
				job.setOutput(HdfsUtil.path(output, chr));
				job.setRefPanel(reference);
				job.setLocalOutput(local);
				job.setLogFilename(FileUtil.path(log, "chr_" + chr + ".log"));
				job.setPhasing(phasing);
				job.setJarByClass(ImputationJob.class);
				job.setNoCache(noCache);
				job.setMinimacBin(minimacBin);

				executeJarInBackground(chr, context, job);
				jobs.put(chr, job);

			}

			waitForAll();
			context.println("All jobs terminated.");

			// canceled by user
			if (isCanceled()) {
				context.println("Canceled by user.");
				updateProgress();
				printSummary(context);

				message.setType(Message.ERROR);
				message.setMessage("Canceled by user.");
				return false;

			}

			// one job was failed
			if (error) {
				context.println("Imputation on chromosome " + errorChr
						+ " failed. Imputation was stopped.");
				updateProgress();
				printSummary(context);

				message.setType(Message.ERROR);
				message.setMessage("Imputation on chromosome " + errorChr
						+ " failed. Imputation was stopped.");
				return false;

			}

			// everthing fine

			updateProgress();
			printSummary(context);

			updateMessage();
			message.setType(Message.OK);
			return true;

		} catch (IOException e1) {

			// unexpected exception

			updateProgress();
			printSummary(context);

			message.setType(Message.ERROR);
			message.setMessage(e1.getMessage());
			return false;

		} catch (InterruptedException e1) {

			// canceled by user

			updateProgress();
			printSummary(context);

			message.setType(Message.ERROR);
			message.setMessage("Canceled by user.");
			return false;

		}

	}

	// print summary and download log files from tasktracker

	private void printSummary(CloudgeneContext context) {
		context.println("Summary: ");
		String log = context.get("hadooplogs");

		for (String id : jobs.keySet()) {

			HadoopJob job = jobs.get(id);
			Integer state = getState(job);

			job.downloadFailedLogs(log);

			if (state != null) {

				if (state == OK) {

					context.println("  [OK]   Chr " + id + " ("
							+ job.getJobId() + ")");

				} else if (state == FAILED) {

					context.println("  [FAIL] Chr " + id + " ("
							+ job.getJobId() + ")");

				} else {
					context.println("  [" + state + "]   Chr " + id + " ("
							+ job.getJobId() + ")");
				}

			} else {

				context.println("  [??]   Chr " + id + " (" + job.getJobId()
						+ ")");

			}

		}

	}

	// update message

	private synchronized void updateMessage() {

		String text = "";

		int i = 1;

		for (String id : jobs.keySet()) {

			HadoopJob job = jobs.get(id);
			Integer state = getState(job);

			if (state != null) {

				if (state == OK) {

					text += "<span class=\"badge badge-success\" style=\"width: 40px\">Chr "
							+ id + "</span>";

				}
				if (state == RUNNING) {

					text += "<span class=\"badge badge-info\" style=\"width: 40px\">Chr "
							+ id + "</span>";

				}
				if (state == FAILED) {

					text += "<span class=\"badge badge-important\" style=\"width: 40px\">Chr "
							+ id + "</span>";

				}
				if (state == WAIT) {

					text += "<span class=\"badge\" style=\"width: 40px\">Chr "
							+ id + "</span>";

				}
			} else {
				text += "<span class=\"badge\" style=\"width: 40px\">Chr " + id
						+ "</span>";
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
	protected synchronized void onJobStart(String id, CloudgeneContext context) {
		context.println("Running job chr_" + id + "....");
	}

	@Override
	protected synchronized void onJobFinish(String id, boolean successful,
			CloudgeneContext context) {

		HadoopJob job = jobs.get(id);

		if (successful) {

			// everything fine

			context.println("Job chr_" + id + " (" + job.getJobId()
					+ ") executed sucessful.");
		} else {

			// one job failed

			context.println("Job chr_" + id + " (" + job.getJobId()
					+ ") failed.");

			// kill all running jobs

			if (!error && !isCanceled()) {
				error = true;
				errorChr = id;
				context.println("Kill all running jobs...");
				kill();
			}
		}

	}

	@Override
	public void updateProgress() {

		super.updateProgress();

		updateMessage();

	}

}
