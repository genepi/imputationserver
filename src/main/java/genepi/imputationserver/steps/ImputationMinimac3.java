package genepi.imputationserver.steps;

import genepi.hadoop.HadoopJob;
import genepi.hadoop.HdfsUtil;
import genepi.hadoop.common.ContextLog;
import genepi.hadoop.common.WorkflowContext;
import genepi.imputationserver.steps.imputationMinimac3.ImputationJobMinimac3;
import genepi.imputationserver.util.GeneticMap;
import genepi.imputationserver.util.MapList;
import genepi.imputationserver.util.ParallelHadoopJobStep;
import genepi.imputationserver.util.RefPanel;
import genepi.imputationserver.util.RefPanelList;
import genepi.io.FileUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ImputationMinimac3 extends ParallelHadoopJobStep {

	Map<String, HadoopJob> jobs = null;

	boolean error = false;

	private WorkflowContext context;

	private String errorChr = "";

	public ImputationMinimac3() {
		super(10);
		jobs = new HashMap<String, HadoopJob>();
	}

	@Override
	public void setup(WorkflowContext context) {

		this.context = context;
	}

	@Override
	public boolean run(WorkflowContext context) {

		String folder = getFolder(ImputationMinimac3.class);

		// inputs
		String input = context.get("mafchunkfile");
		String reference = context.get("refpanel");
		String phasing = context.get("phasing");
		String rounds = context.get("rounds");
		String window = context.get("window");
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
		String log = context.get("logfile");

		if (!HdfsUtil.exists(input)) {
			context.error("No chunks passed the QC step.");
			return false;
		}

		// load reference panels

		RefPanelList panels = null;
		try {
			panels = RefPanelList.loadFromFile(FileUtil.path(folder,
					"panels.txt"));

		} catch (Exception e) {

			context.error("panels.txt not found.");
			return false;
		}

		// check reference panel

		RefPanel panel = panels.getById(reference);
		if (panel == null) {
			context.error("Reference Panel '" + reference + "' not found.");
			return false;
		}

		// load maps

		MapList maps = null;
		try {
			maps = MapList.loadFromFile(FileUtil.path(folder,
					"genetic-maps.txt"));

		} catch (Exception e) {

			context.error("genetic-maps.txt not found.");
			return false;
		}

		// check map for hapmap2
		GeneticMap map = maps.getById("hapmap2");
		if (map == null) {
			context.error("hapmap2 not found.");
			return false;
		}

		// execute one job per chromosome

		try {
			List<String> chunkFiles = HdfsUtil.getFiles(input);

			context.beginTask("Start Imputation...");

			for (String chunkFile : chunkFiles) {

				String[] tiles = chunkFile.split("/");
				String chr = tiles[tiles.length - 1];

				ImputationJobMinimac3 job = new ImputationJobMinimac3(
						context.getJobName() + "-chr-" + chr, new ContextLog(
								context));
				job.setFolder(folder);
				job.setRefPanelHdfs(panel.getHdfs());
				job.setRefPanelPattern(panel.getPattern());

				job.setMapShapeITHdfs(map.getMapShapeIT());
				job.setMapShapeITPattern(map.getMapPatternShapeIT());

				job.setMapHapiURHdfs(map.getMapHapiUR());
				job.setMapHapiURPattern(map.getMapPatternHapiUR());

				job.setInput(chunkFile);
				job.setOutput(HdfsUtil.path(output, chr));
				job.setRefPanel(reference);
				job.setLogFilename(FileUtil.path(log, "chr_" + chr + ".log"));
				job.setPhasing(phasing);
				job.setRounds(rounds);
				job.setWindow(window);
				job.setJarByClass(ImputationJobMinimac3.class);
				job.setNoCache(noCache);
				job.setMinimacBin(minimacBin);

				executeJarInBackground(chr, context, job);
				jobs.put(chr, job);

			}

			waitForAll();
			context.println("All jobs terminated.");

			// one job was failed
			if (error) {
				context.println("Imputation on chromosome " + errorChr
						+ " failed. Imputation was stopped.");
				updateProgress();
				printSummary();

				context.error("Imputation on chromosome " + errorChr
						+ " failed. Imputation was stopped.");
				return false;

			}

			// canceled by user
			if (isCanceled()) {
				context.println("Canceled by user.");
				updateProgress();
				printSummary();

				context.error("Canceled by user.");

				return false;

			}

			// everthing fine

			updateProgress();
			printSummary();

			String text = updateMessage();
			context.updateTask(text, WorkflowContext.OK);

			return true;

		} catch (IOException e1) {

			// unexpected exception

			updateProgress();
			printSummary();

			context.updateTask(e1.getMessage(), WorkflowContext.ERROR);
			return false;

		} catch (InterruptedException e1) {

			// canceled by user

			updateProgress();
			printSummary();

			context.updateTask("Canceled by user.", WorkflowContext.ERROR);
			return false;

		}

	}

	// print summary and download log files from tasktracker

	private void printSummary() {
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

	private synchronized String updateMessage() {

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

		return text;

	}

	@Override
	protected synchronized void onJobStart(String id, WorkflowContext context) {
		context.println("Running job chr_" + id + "....");
	}

	@Override
	protected synchronized void onJobFinish(String id, boolean successful,
			WorkflowContext context) {

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

		String text = updateMessage();
		context.updateTask(text, WorkflowContext.RUNNING);

	}

}
