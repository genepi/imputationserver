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

		// outputs
		String output = context.get("outputimputation");
		String local = context.get("local");
		String log = context.get("logfile");

		if (!HdfsUtil.exists(input)) {
			error("No chunks passed the QC step.");
			return false;
		}

		try {
			List<String> chunkFiles = HdfsUtil.getFiles(input);

			message = createLogMessage("", Message.OK);

			// load reference panels

			RefPanelList panels = null;
			try {
				panels = RefPanelList.loadFromFile(FileUtil.path(folder,
						"panels.txt"));

			} catch (Exception e) {

				error("panels.txt not found.");
				return false;
			}

			for (String chunkFile : chunkFiles) {

				String[] tiles = chunkFile.split("/");
				String chr = tiles[tiles.length - 1];

				ImputationJob job = new ImputationJob("");
				job.setFolder(folder);

				RefPanel panel = panels.getById(reference);
				if (panel == null) {
					error("Reference '" + reference + "' not found.");
					error("Available references:");
					for (RefPanel p : panels.getPanels()) {
						error(p.getId());
					}

					return false;
				}

				job.setRefPanelHdfs(panel.getHdfs());
				job.setRefPanelPattern(panel.getPattern());
				job.setInput(chunkFile);
				job.setOutput(HdfsUtil.path(output, chr));
				job.setRefPanel(reference);
				job.setLocalOutput(local);
				job.setLogFilename(FileUtil.path(log, "chr_" + chr + ".log"));
				job.setPhasing(phasing);
				job.setJarByClass(ImputationJob.class);

				executeJarInBackground(chr, context, job);
				jobs.put(chr, job);

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
	protected synchronized void onJobStart(String id) {
	}

	@Override
	protected synchronized void onJobFinish(String id, boolean successful) {

		if (!successful) {
			kill();
		}

	}

	@Override
	public void updateProgress() {

		super.updateProgress();

		updateMessage();

	}

}
