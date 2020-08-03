package genepi.imputationserver.steps;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import cloudgene.sdk.internal.WorkflowContext;
import genepi.hadoop.HadoopJob;
import genepi.hadoop.HdfsUtil;
import genepi.hadoop.io.HdfsLineWriter;
import genepi.imputationserver.steps.imputation.ImputationJob;
import genepi.imputationserver.steps.vcf.VcfChunk;
import genepi.imputationserver.util.ContextLog;
import genepi.imputationserver.util.DefaultPreferenceStore;
import genepi.imputationserver.util.ParallelHadoopJobStep;
import genepi.imputationserver.util.PgsPanel;
import genepi.imputationserver.util.RefPanel;
import genepi.imputationserver.util.RefPanelList;
import genepi.io.FileUtil;
import genepi.io.text.LineReader;

public class Imputation extends ParallelHadoopJobStep {

	Map<String, HadoopJob> jobs = null;

	boolean error = false;

	private WorkflowContext context;

	private String errorChr = "";

	private boolean running = true;

	private String output;

	private String outputScores;

	private boolean ok = false;

	public static int THREADS = 25;

	public Imputation() {
		super(THREADS);
		jobs = new HashMap<String, HadoopJob>();
	}

	@Override
	public void setup(WorkflowContext context) {

		this.context = context;
	}

	@Override
	public boolean run(WorkflowContext context) {

		final String folder = getFolder(Imputation.class);

		// inputs
		String input = context.get("chunkFileDir");
		String reference = context.get("refpanel");
		String binariesHDFS = context.getConfig("binaries");
		String mode = context.get("mode");
		String phasing = context.get("phasing");
		PgsPanel pgsPanel = PgsPanel.loadFromProperties(context.getData("pgsPanel"));

		String r2Filter = context.get("r2Filter");
		if (r2Filter == null) {
			r2Filter = "0";
		}

		// outputs
		output = context.get("outputimputation");
		// output scores
		outputScores = context.get("outputScores");

		String log = context.get("logfile");

		if (!(new File(input)).exists()) {
			context.error("No chunks passed the QC step.");
			return false;
		}

		// load reference panels

		RefPanelList panels = RefPanelList.loadFromFile(FileUtil.path(folder, RefPanelList.FILENAME));

		RefPanel panel = null;
		try {
			panel = panels.getById(reference, context.getData("refpanel"));
			if (panel == null) {
				context.error("reference panel '" + reference + "' not found.");
				return false;
			}
		} catch (Exception e) {
			context.error("Unable to parse reference panel '" + reference + "': " + e.getMessage());
			return false;
		}

		context.println("Reference Panel: ");
		context.println("  Name: " + reference);
		context.println("  ID: " + panel.getId());
		context.println("  Build: " + panel.getBuild());
		context.println("  Location: " + panel.getHdfs());
		context.println("  Legend: " + panel.getLegend());
		context.println("  Version: " + panel.getVersion());
		context.println("  Eagle Map: " + panel.getMapEagle());
		context.println("  Eagle BCFs: " + panel.getRefEagle());
		context.println("  Beagle Bref3: " + panel.getRefBeagle());
		context.println("  Beagle Map: " + panel.getMapBeagle());
		context.println("  Minimac Map: " + panel.getMapMinimac());
		context.println("  Populations:");
		for (Map.Entry<String, String> entry : panel.getPopulations().entrySet()) {
			context.println("    " + entry.getKey() + "/" + entry.getValue());
		}
		context.println("  Samples:");
		for (Map.Entry<String, String> entry : panel.getSamples().entrySet()) {
			context.println("    " + entry.getKey() + "/" + entry.getValue());
		}
		if (panel.getQcFilter() != null) {
			context.println("  QC Filters:");
			for (Map.Entry<String, String> entry : panel.getQcFilter().entrySet()) {
				context.println("    " + entry.getKey() + "/" + entry.getValue());
			}
		}
		if (pgsPanel != null) {
			context.println("  PGS: " + pgsPanel.getScores().size() + " scores");
		} else {
			context.println("  PGS: no scores selected");
		}

		// execute one job per chromosome
		try {
			String[] chunkFiles = FileUtil.getFiles(input, "*.*");

			context.beginTask("Start Imputation...");

			if (chunkFiles.length == 0) {
				context.error("<br><b>Error:</b> No chunks found. Imputation cannot be started!");
				return false;
			}

			for (String chunkFile : chunkFiles) {

				String[] tiles = chunkFile.split("/");
				String chr = tiles[tiles.length - 1];

				ChunkFileConverterResult result = convertChunkfile(chunkFile, context.getHdfsTemp());

				ImputationJob job = new ImputationJob(context.getJobId() + "-chr-" + chr, new ContextLog(context)) {
					@Override
					protected void readConfigFile() {
						File file = new File(folder + "/" + CONFIG_FILE);
						DefaultPreferenceStore preferenceStore = new DefaultPreferenceStore();
						if (file.exists()) {
							log.info("Loading distributed configuration file '" + file.getAbsolutePath() + "'...");
							preferenceStore.load(file);

						} else {
							log.info("Configuration file '" + file.getAbsolutePath()
									+ "' not available. Use default values");

						}

						preferenceStore.write(getConfiguration());
						for (Object key : preferenceStore.getKeys()) {
							log.info("  " + key + ": " + preferenceStore.getString(key.toString()));
						}

					}
				};

				job.setBinariesHDFS(binariesHDFS);

				String hdfsFilenameChromosome = resolvePattern(panel.getHdfs(), chr);
				job.setRefPanelHdfs(hdfsFilenameChromosome);

				job.setR2Filter(r2Filter);
				job.setBuild(panel.getBuild());
				if (panel.getMapMinimac() != null) {
					context.println("Setting up minimac map file...");
					job.setMapMinimac(panel.getMapMinimac());
				} else {
					context.println("Reference panel has no minimac map file.");
				}

				if (result.needsPhasing) {
					context.println("Input data is unphased.");

					if (phasing.equals("beagle")) {
						context.println("  Setting up beagle reference and map files...");
						String refBeagleFilenameChromosome = resolvePattern(panel.getRefBeagle(), chr);
						String mapBeagleFilenameChromosome = resolvePattern(panel.getMapBeagle(), chr);
						job.setRefBeagleHdfs(refBeagleFilenameChromosome);
						job.setMapBeagleHdfs(mapBeagleFilenameChromosome);
					} else {

						if (!panel.checkEagleMap()) {
							context.error("Eagle map file not found.");
							return false;
						}

						context.println("  Setting up eagle reference and map files...");
						job.setMapEagleHdfs(panel.getMapEagle());
						String refEagleFilenameChromosome = resolvePattern(panel.getRefEagle(), chr);
						job.setRefEagleHdfs(refEagleFilenameChromosome);
					}

				} else {
					context.println("Input data is phased.");
				}

				if (mode != null && mode.equals("phasing")) {
					job.setPhasingOnly("true");
				} else {
					job.setPhasingOnly("false");
				}

				job.setPhasingEngine(phasing);
				job.setInput(result.filename);
				job.setOutput(HdfsUtil.path(output, chr));

				if (outputScores != null) {
					job.setOutputScores(outputScores);
				}

				if (pgsPanel != null) {
					job.setScores(pgsPanel.getScores());
				}
				job.setRefPanel(reference);
				job.setLogFilename(FileUtil.path(log, "chr_" + chr + ".log"));
				job.setJarByClass(ImputationJob.class);

				executeJarInBackground(chr, context, job);
				jobs.put(chr, job);

			}

			waitForAll();
			running = false;
			context.println("All jobs terminated.");

			// one job was failed
			if (error) {
				context.println("Imputation on chromosome " + errorChr + " failed. Imputation was stopped.");
				updateProgress();

				String text = updateMessage();
				context.endTask(text, WorkflowContext.ERROR);

				printSummary();

				context.error("Imputation on chromosome " + errorChr + " failed. Imputation was stopped.");
				return false;

			}

			// canceled by user
			if (isCanceled()) {
				context.println("Canceled by user.");

				updateProgress();

				String text = updateMessage();
				context.endTask(text, WorkflowContext.ERROR);

				printSummary();

				context.error("Canceled by user.");

				return false;

			}

			// everything fine

			updateProgress();
			printSummary();

			String text = updateMessage();
			context.endTask(text, ok ? WorkflowContext.OK : WorkflowContext.ERROR);

			return ok;

		} catch (Exception e) {

			// unexpected exception

			updateProgress();
			printSummary();
			e.printStackTrace();
			context.updateTask(e.getMessage(), WorkflowContext.ERROR);
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

			try {
				job.downloadFailedLogs(log);
			} catch (Exception e) {
				context.println("[INFO] Error while downloading log files");
			}

			if (state != null) {

				if (state == OK) {

					context.println("  [OK]   Chr " + id + " (" + job.getJobId() + ")");

				} else if (state == FAILED) {

					context.println("  [FAIL] Chr " + id + " (" + job.getJobId() + ")");

				} else {
					context.println("  [" + state + "]   Chr " + id + " (" + job.getJobId() + ")");
				}

			} else {

				context.println("  [??]   Chr " + id + " (" + job.getJobId() + ")");

			}

		}

	}

	// update message

	private synchronized String updateMessage() {

		String text = "";
		String text2 = "";

		int i = 1;

		for (String id : jobs.keySet()) {

			HadoopJob job = jobs.get(id);
			Integer state = getState(job);

			if (state != null) {

				if (id.equals("X.PAR1")) {
					text2 = "X1";
				} else if (id.equals("X.nonPAR")) {
					text2 = "X2";
				} else if (id.equals("X.PAR2")) {
					text2 = "X3";
				} else {
					text2 = id;
				}

				if (state == OK) {

					text += "<span class=\"badge badge-success\" style=\"width: 40px\">Chr " + text2 + "</span>";

				}
				if (state == RUNNING) {

					text += "<span class=\"badge badge-info\" style=\"width: 40px\">Chr " + text2 + "</span>";

				}
				if (state == FAILED) {

					text += "<span class=\"badge badge-important\" style=\"width: 40px\">Chr " + text2 + "</span>";

				}
				if (state == WAIT) {

					text += "<span class=\"badge\" style=\"width: 40px\">Chr " + text2 + "</span>";

				}
			} else {
				text += "<span class=\"badge\" style=\"width: 40px\">Chr " + text2 + "</span>";
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
	protected synchronized void onJobFinish(String id, boolean successful, WorkflowContext context) {

		HadoopJob job = jobs.get(id);

		if (successful) {

			// everything fine
			ok = true;
			context.println("Job chr_" + id + " (" + job.getJobId() + ") executed sucessful.");
		} else {

			// one job failed

			context.println("Job chr_" + id + " (" + job.getJobId() + ") failed.");

			// kill all running jobs

			if (!error && !isCanceled() && !id.startsWith("X.")) {
				error = true;
				errorChr = id;
				context.println("Kill all running jobs...");
				kill();
			}

			// if chr X --> delete results
			if (id.startsWith("X.")) {
				String outputFolder = HdfsUtil.path(output, id);
				context.println("Delete outpufolder for " + id + ": " + outputFolder);
				HdfsUtil.delete(outputFolder);
			}

		}

	}

	@Override
	public void updateProgress() {

		super.updateProgress();
		if (running) {
			String text = updateMessage();
			context.updateTask(text, WorkflowContext.RUNNING);
		}

	}

	class ChunkFileConverterResult {
		public String filename;

		public boolean needsPhasing;
	}

	private ChunkFileConverterResult convertChunkfile(String chunkFile, String output) throws IOException {

		String name = FileUtil.getFilename(chunkFile);
		String newChunkFile = HdfsUtil.path(output, name);

		LineReader reader = new LineReader(chunkFile);
		HdfsLineWriter writer = new HdfsLineWriter(newChunkFile);

		boolean phased = true;

		while (reader.next()) {
			VcfChunk chunk = new VcfChunk(reader.get());

			phased = phased && chunk.isPhased();

			// put vcf file
			String sourceVcf = chunk.getVcfFilename();
			String targetVcf = HdfsUtil.path(output, FileUtil.getFilename(sourceVcf));
			HdfsUtil.put(sourceVcf, targetVcf);
			chunk.setVcfFilename(targetVcf);

			writer.write(chunk.serialize());

		}
		reader.close();
		writer.close();

		ChunkFileConverterResult result = new ChunkFileConverterResult();
		result.filename = newChunkFile;
		result.needsPhasing = !phased;
		return result;

	}

	private String resolvePattern(String pattern, String chr) {
		return pattern.replaceAll("\\$chr", chr);
	}

}
