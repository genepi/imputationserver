package genepi.imputationserver.steps;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;

import cloudgene.sdk.internal.WorkflowContext;
import cloudgene.sdk.internal.WorkflowStep;
import genepi.hadoop.importer.IImporter;
import genepi.hadoop.importer.ImporterFactory;
import genepi.imputationserver.steps.imputation.ImputationPipeline;
import genepi.imputationserver.steps.vcf.VcfFile;
import genepi.imputationserver.steps.vcf.VcfFileUtil;
import genepi.imputationserver.util.DefaultPreferenceStore;
import genepi.imputationserver.util.RefPanel;
import genepi.imputationserver.util.RefPanelList;
import genepi.io.FileUtil;

public class InputValidation extends WorkflowStep {

	@Override
	public boolean run(WorkflowContext context) {

		context.log("Versions:");
		context.log("  Pipeline: " + ImputationPipeline.PIPELINE_VERSION);
		context.log("  Imputation-Engine: " + ImputationPipeline.IMPUTATION_VERSION);
		context.log("  Phasing-Engine: " + ImputationPipeline.PHASING_VERSION);

		if (!checkParameters(context)) {
			return false;
		}

		if (!importVcfFiles(context)) {
			return false;
		}

		return checkVcfFiles(context);

	}

	protected void setupTabix(String folder) {
		VcfFileUtil.setTabixBinary(FileUtil.path(folder, "bin", "tabix"));
	}

	private boolean checkVcfFiles(WorkflowContext context) {
		String folder = getFolder(InputValidation.class);
		setupTabix(folder);
		String files = context.get("files");
		String reference = context.get("refpanel");
		String population = context.get("population");
		String build = context.get("build");
		String r2Filter = context.get("r2Filter");
		String phasing = context.get("phasing");
		String mode = context.get("mode");

		// load job.config
		File jobConfig = new File(FileUtil.path(folder, "job.config"));
		DefaultPreferenceStore store = new DefaultPreferenceStore();
		if (jobConfig.exists()) {
			store.load(jobConfig);
		} else {
			context.log("Configuration file '" + jobConfig.getAbsolutePath() + "' not available. Use default values.");
		}

		int chunkSize = 20000000;
		if (store.getString("chunksize") != null) {
			chunkSize = Integer.parseInt(store.getString("chunksize"));
		}

		int maxSamples = 0;
		if (store.getString("samples.max") != null) {
			maxSamples = Integer.parseInt(store.getString("samples.max"));
		}

		List<VcfFile> validVcfFiles = new Vector<VcfFile>();

		context.beginTask("Analyze files ");
		List<String> chromosomes = new Vector<String>();

		int chunks = 0;
		int noSnps = 0;
		int noSamples = 0;

		boolean phased = true;

		String[] vcfFiles = FileUtil.getFiles(files, "*.vcf.gz$|*.vcf$");

		if (vcfFiles.length == 0) {
			context.endTask("The provided files are not VCF files (see <a href=\"/start.html#!pages/help\">Help</a>).",
					WorkflowContext.ERROR);
			return false;
		}

		Arrays.sort(vcfFiles);

		String infos = null;

		RefPanelList panels = RefPanelList.loadFromFile(FileUtil.path(folder, RefPanelList.FILENAME));

		RefPanel panel = null;
		try {
			panel = panels.getById(reference, context.getData("refpanel"));
			if (panel == null) {
				context.endTask("Reference '" + reference + "' not found.", WorkflowContext.ERROR);
				return false;
			}
		} catch (Exception e) {
			context.error("Unable to parse reference panel '" + reference + "': " + e.getMessage());
			return false;
		}

		for (String filename : vcfFiles) {

			if (infos == null) {
				// first files, no infos available
				context.updateTask("Analyze file " + FileUtil.getFilename(filename) + "...", WorkflowContext.RUNNING);

			} else {
				context.updateTask("Analyze file " + FileUtil.getFilename(filename) + "...\n\n" + infos,
						WorkflowContext.RUNNING);
			}

			try {

				VcfFile vcfFile = VcfFileUtil.load(filename, chunkSize, true);

				if (VcfFileUtil.isChrMT(vcfFile.getChromosome())) {
					vcfFile.setPhased(true);
				}

				if (VcfFileUtil.isValidChromosome(vcfFile.getChromosome())) {

					validVcfFiles.add(vcfFile);
					chromosomes.add(vcfFile.getChromosome());

					String chromosomeString = "";
					for (String chr : chromosomes) {
						chromosomeString += " " + chr;
					}

					// check if all files have same amount of samples
					if (noSamples != 0 && noSamples != vcfFile.getNoSamples()) {
						context.endTask(
								"Please double check, if all uploaded VCF files include the same amount of samples ("
										+ vcfFile.getNoSamples() + " vs " + noSamples + ")",
								WorkflowContext.ERROR);
						return false;
					}

					noSamples = vcfFile.getNoSamples();
					noSnps += vcfFile.getNoSnps();
					chunks += vcfFile.getChunks().size();

					phased = phased && vcfFile.isPhased();

					if (vcfFile.isPhasedAutodetect() && !vcfFile.isPhased()) {

						context.endTask(
								"File should be phased, but also includes unphased and/or missing genotypes! Please double-check!",
								WorkflowContext.ERROR);
						return false;
					}

					if (noSamples > maxSamples && maxSamples != 0) {

						String contactName = store.getString("contact.name");
						String contactEmail = store.getString("contact.email");

						context.endTask("The maximum number of samples is " + maxSamples + ". Please contact "
								+ contactName + " (<a href=\"" + contactEmail + "\">" + contactEmail
								+ "</a>) to discuss this large imputation.", WorkflowContext.ERROR);

						return false;
					}

					if (build == null) {
						build = "hg19";
					}

					if (build.equals("hg19") && vcfFile.hasChrPrefix()) {
						context.endTask("Your upload data contains chromosome '" + vcfFile.getRawChromosome()
								+ "'. This is not a valid hg19 encoding. Please ensure that your input data is build hg19 and chromosome is encoded as '"
								+ vcfFile.getChromosome() + "'.", WorkflowContext.ERROR);
						return false;
					}

					if (build.equals("hg38") && !vcfFile.hasChrPrefix()) {
						context.endTask("Your upload data contains chromosome '" + vcfFile.getRawChromosome()
								+ "'. This is not a valid hg38 encoding. Please ensure that your input data is build hg38 and chromosome is encoded as 'chr"
								+ vcfFile.getChromosome() + "'.", WorkflowContext.ERROR);
						return false;
					}

					infos = "Samples: " + noSamples + "\n" + "Chromosomes:" + chromosomeString + "\n" + "SNPs: "
							+ noSnps + "\n" + "Chunks: " + chunks + "\n" + "Datatype: "
							+ (phased ? "phased" : "unphased") + "\n" + "Build: " + (build == null ? "hg19" : build)
							+ "\n" + "Reference Panel: " + reference + " (" + panel.getBuild() + ")" + "\n"
							+ "Population: " + population + "\n" + "Phasing: eagle" + "\n" + "Mode: " + mode;

					if (r2Filter != null && !r2Filter.isEmpty() && !r2Filter.equals("0")) {
						infos += "\nRsq filter: " + r2Filter;
					}

				} else {
					context.endTask("No valid chromosomes found!", WorkflowContext.ERROR);
					return false;
				}

			} catch (IOException e) {

				context.endTask(e.getMessage() + " (see <a href=\"/start.html#!pages/help\">Help</a>).",
						WorkflowContext.ERROR);
				return false;

			}

		}

		if (validVcfFiles.size() > 0) {

			context.endTask(validVcfFiles.size() + " valid VCF file(s) found.\n\n" + infos, WorkflowContext.OK);

			if (!phased && (phasing == null || phasing.isEmpty() || phasing.equals("no_phasing"))) {
				context.error("Your input data is unphased. Please select an algorithm for phasing.");
				return false;
			}

			// init counters
			context.incCounter("samples", noSamples);
			context.incCounter("genotypes", noSamples * noSnps);
			context.incCounter("chromosomes", noSamples * chromosomes.size());
			context.incCounter("runs", 1);
			context.incCounter("refpanel_" + reference, 1);
			context.incCounter("phasing_" + "eagle", 1);

			return true;

		} else {

			context.endTask("The provided files are not VCF files  (see <a href=\"/start.html#!pages/help\">Help</a>).",
					WorkflowContext.ERROR);

			return false;
		}
	}

	private boolean checkParameters(WorkflowContext context) {

		String folder = getFolder(InputValidation.class);
		setupTabix(folder);
		String reference = context.get("refpanel");
		String population = context.get("population");

		RefPanelList panels = RefPanelList.loadFromFile(FileUtil.path(folder, RefPanelList.FILENAME));

		try {

			RefPanel panel = panels.getById(reference, context.getData("refpanel"));
			if (panel == null) {
				StringBuilder report = new StringBuilder();
				report.append("Reference '" + reference + "' not found.\n");
				report.append("Available reference IDs:");
				for (RefPanel p : panels.getPanels()) {
					report.append("\n - " + p.getId());
				}
				context.error(report.toString());
				return false;
			}

			if (!panel.supportsPopulation(population)) {
				StringBuilder report = new StringBuilder();
				report.append(
						"Population '" + population + "' is not supported by reference panel '" + reference + "'.\n");
				if (panel.getPopulations() != null) {
					report.append("Available populations:");
					for (String pop : panel.getPopulations().values()) {
						report.append("\n - " + pop);
					}
				}
				context.error(report.toString());
				return false;
			}

		} catch (Exception e) {
			context.error("Unable to parse reference panel '" + reference + "': " + e.getMessage());
			return false;
		}

		return true;
	}

	private boolean importVcfFiles(WorkflowContext context) {

		for (String input : context.getInputs()) {

			if (ImporterFactory.needsImport(context.get(input))) {

				context.beginTask("Importing files...");

				String[] urlList = context.get(input).split(";")[0].split("\\s+");

				String username = "";
				if (context.get(input).split(";").length > 1) {
					username = context.get(input).split(";")[1];
				}

				String password = "";
				if (context.get(input).split(";").length > 2) {
					password = context.get(input).split(";")[2];
				}

				for (String url2 : urlList) {

					String url = url2 + ";" + username + ";" + password;
					String target = FileUtil.path(context.getLocalTemp(), "importer", input);
					FileUtil.createDirectory(target);
					context.println("Import to local workspace " + target + "...");

					try {

						context.updateTask("Import " + url2 + "...", WorkflowContext.RUNNING);
						context.log("Import " + url2 + "...");
						IImporter importer = ImporterFactory.createImporter(url, target);

						if (importer != null) {

							boolean successful = importer.importFiles("vcf.gz");

							if (successful) {

								context.setInput(input, target);

							} else {

								context.updateTask("Import " + url2 + " failed: " + importer.getErrorMessage(),
										WorkflowContext.ERROR);

								return false;

							}

						} else {

							context.updateTask("Import " + url2 + " failed: Protocol not supported",
									WorkflowContext.ERROR);

							return false;

						}

					} catch (Exception e) {
						context.updateTask("Import File(s) " + url2 + " failed: " + e.toString(),
								WorkflowContext.ERROR);

						return false;
					}

				}

				context.updateTask("File Import successful. ", WorkflowContext.OK);

			}

		}

		return true;

	}

}
