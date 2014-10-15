package genepi.imputationserver.steps;

import genepi.hadoop.HdfsUtil;
import genepi.hadoop.common.WorkflowContext;
import genepi.hadoop.common.WorkflowStep;
import genepi.imputationserver.steps.vcf.VcfFile;
import genepi.imputationserver.steps.vcf.VcfFileUtil;
import genepi.imputationserver.util.RefPanel;
import genepi.imputationserver.util.RefPanelList;
import genepi.io.FileUtil;

import java.io.IOException;
import java.util.List;
import java.util.Vector;

import cloudgene.mapred.steps.importer.IImporter;
import cloudgene.mapred.steps.importer.ImporterFactory;

public class InputValidation extends WorkflowStep {

	@Override
	public boolean run(WorkflowContext context) {

		if (!importVcfFiles(context)) {
			return false;
		}

		return checkVcfFiles(context);

	}

	private boolean checkVcfFiles(WorkflowContext context) {
		String folder = getFolder(InputValidation.class);

		// inputs
		String inputFiles = context.get("files");
		String reference = context.get("refpanel");
		int chunkSize = Integer.parseInt(context.get("chunksize"));

		String tabix = FileUtil.path(folder, "bin", "tabix");
		String files = FileUtil.path(context.getLocalTemp(), "input");

		// exports files from hdfs
		try {

			HdfsUtil.getFolder(inputFiles, files);

		} catch (Exception e) {
			context.error("Downloading files: " + e.getMessage());
			return false;

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
			context.endTask(
					"The provided files are not VCF files (see <a href=\"/start.html#!pages/help\">Help</a>).",
					WorkflowContext.ERROR);
			return false;
		}

		String infos = null;

		for (String filename : vcfFiles) {

			if (infos == null) {
				// first files, no infos available
				context.updateTask(
						"Analyze file " + FileUtil.getFilename(filename)
								+ "...", WorkflowContext.RUNNING);

			} else {
				context.updateTask(
						"Analyze file " + FileUtil.getFilename(filename)
								+ "...\n\n" + infos, WorkflowContext.RUNNING);
			}

			try {

				VcfFile vcfFile = VcfFileUtil.load(filename, chunkSize, tabix);

				if (VcfFileUtil.isAutosomal(vcfFile.getChromosome())) {

					validVcfFiles.add(vcfFile);
					chromosomes.add(vcfFile.getChromosome());
					String chromosomeString = "";
					for (String chr : chromosomes) {
						chromosomeString += " " + chr;
					}
					noSamples = vcfFile.getNoSamples();
					noSnps += vcfFile.getNoSnps();
					chunks += vcfFile.getChunks().size();

					phased = phased && vcfFile.isPhased();

					// check reference panel

					if (vcfFile.isPhasedAutodetect() && !vcfFile.isPhased()) {

						context.endTask(
								"File should be phased, but also includes unphased and/or missing genotypes! Please double-check!",
								WorkflowContext.ERROR);
						return false;
					}

					RefPanelList panels = null;
					try {
						panels = RefPanelList.loadFromFile(FileUtil.path(
								folder, "panels.txt"));

					} catch (Exception e) {

						context.endTask("panels.txt not found.",
								WorkflowContext.ERROR);
						return false;
					}
					RefPanel panel = panels.getById(reference);
					if (panel == null) {
						context.endTask("Reference '" + reference
								+ "' not found.", WorkflowContext.ERROR);

						return false;
					}

					infos = "Samples: " + noSamples + "\n" + "Chromosomes:"
							+ chromosomeString + "\n" + "SNPs: " + noSnps
							+ "\n" + "Chunks: " + chunks + "\n" + "Datatype: "
							+ (phased ? "phased" : "unphased") + "\n"
							+ "Reference Panel: " + panel.getId();

				}

			} catch (IOException e) {

				context.endTask(
						e.getMessage()
								+ " (see <a href=\"/start.html#!pages/help\">Help</a>).",
						WorkflowContext.ERROR);
				return false;

			}

		}

		if (validVcfFiles.size() > 0) {

			context.endTask(validVcfFiles.size()
					+ " valid VCF file(s) found.\n\n" + infos,
					WorkflowContext.OK);

			// init counteres
			context.incCounter("samples", noSamples);
			context.incCounter("genotypes", noSamples * noSnps);
			context.incCounter("chromosomes", noSamples * chromosomes.size());
			context.incCounter("runs", 1);
			context.incCounter("refpanel_" + reference, 1);

			return true;

		} else {

			context.endTask(
					"The provided files are not VCF files  (see <a href=\"/start.html#!pages/help\">Help</a>).",
					WorkflowContext.ERROR);

			return false;
		}
	}

	private boolean importVcfFiles(WorkflowContext context) {

		for (String input : context.getInputs()) {

			if (ImporterFactory.needsImport(context.get(input))) {

				context.beginTask("Importing files...");

				String[] urlList = context.get(input).split(";")[0]
						.split("\\s+");

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

					String target = HdfsUtil.path(context.getHdfsTemp(),
							"importer", input);

					try {

						context.updateTask("Import " + url2 + "...",
								WorkflowContext.RUNNING);

						IImporter importer = ImporterFactory.createImporter(
								url, target);

						if (importer != null) {

							boolean successful = importer.importFiles("vcf.gz");

							if (successful) {

								context.setInput(input, target);

							} else {

								context.updateTask(
										"Import " + url2 + " failed: "
												+ importer.getErrorMessage(),
										WorkflowContext.ERROR);

								return false;

							}

						} else {

							context.updateTask("Import " + url2
									+ " failed: Protocol not supported",
									WorkflowContext.ERROR);

							return false;

						}

					} catch (Exception e) {
						context.updateTask("Import File(s) " + url2
								+ " failed: " + e.toString(),
								WorkflowContext.ERROR);

						return false;
					}

				}

				context.updateTask("File Import successful. ",
						WorkflowContext.OK);

			}

		}

		return true;

	}

}
