package genepi.imputationserver.steps;

import genepi.hadoop.HdfsUtil;
import genepi.hadoop.PreferenceStore;
import genepi.imputationserver.steps.vcf.VcfFile;
import genepi.imputationserver.steps.vcf.VcfFileUtil;
import genepi.imputationserver.util.RefPanel;
import genepi.imputationserver.util.RefPanelList;
import genepi.io.FileUtil;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Vector;

import cloudgene.mapred.jobs.CloudgeneContext;
import cloudgene.mapred.jobs.CloudgeneStep;
import cloudgene.mapred.jobs.Message;
import cloudgene.mapred.steps.importer.IImporter;
import cloudgene.mapred.steps.importer.ImporterFactory;
import cloudgene.mapred.wdl.WdlStep;

public class InputValidation extends CloudgeneStep {

	@Override
	public boolean run(WdlStep step, CloudgeneContext context) {

		if (!importVcfFiles(context)) {
			return false;
		}

		return checkVcfFiles(context);

	}

	private boolean checkVcfFiles(CloudgeneContext context) {
		String folder = getFolder(InputValidation.class);

		// inputs
		String inputFiles = context.get("files");
		String reference = context.get("refpanel");
		int chunkSize = Integer.valueOf(context.get("chunksize"));
		
		// read config
		//PreferenceStore store = new PreferenceStore(new File(FileUtil.path(
		//		folder, "job.config")));
		
		//int chunkSize = Integer.parseInt(store.getString("pipeline.chunksize"));

		String tabix = FileUtil.path(folder, "bin", "tabix");
		String files = FileUtil.path(context.getLocalTemp(), "input");

		// exports files from hdfs
		try {

			HdfsUtil.getFolder(inputFiles, files);

		} catch (Exception e) {
			error("Downloading files: " + e.getMessage());
			return false;

		}

		List<VcfFile> validVcfFiles = new Vector<VcfFile>();

		Message analyzeMessage = createLogMessage("Analyze file ",
				Message.RUNNING);
		List<String> chromosomes = new Vector<String>();

		int chunks = 0;
		int noSnps = 0;
		int noSamples = 0;

		boolean phased = true;

		String[] vcfFiles = FileUtil.getFiles(files, "*.vcf.gz$|*.vcf$");

		if (vcfFiles.length == 0) {
			analyzeMessage.setType(Message.ERROR);
			analyzeMessage
					.setMessage("The provided files are not VCF files (see <a href=\"/start.html#!pages/help\">Help</a>).");
			return false;
		}

		Message chromosomeMessage = createLogMessage("", Message.OK);

		for (String filename : vcfFiles) {
			analyzeMessage.setMessage("Analyze file "
					+ FileUtil.getFilename(filename) + "...");

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
					// load reference panels
					RefPanelList panels = null;
					try {
						panels = RefPanelList.loadFromFile(FileUtil.path(
								folder, "panels.txt"));

					} catch (Exception e) {

						analyzeMessage.setType(Message.ERROR);
						analyzeMessage.setMessage("panels.txt not found.");
						return false;
					}
					RefPanel panel = panels.getById(reference);
					if (panel == null) {
						analyzeMessage.setType(Message.ERROR);
						analyzeMessage.setMessage("Reference '" + reference
								+ "' not found.");
						chromosomeMessage.setType(Message.ERROR);

						return false;
					}

					chromosomeMessage.setMessage("Samples: " + noSamples + "\n"
							+ "Chromosomes:" + chromosomeString + "\n"
							+ "SNPs: " + noSnps + "\n" + "Chunks: " + chunks
							+ "\n" + "Datatype: "
							+ (phased ? "phased" : "unphased") + "\n"
							+ "Reference Panel: " + panel.getId());

				}

			} catch (IOException e) {

				analyzeMessage.setType(Message.ERROR);
				chromosomeMessage.setType(Message.ERROR);
				chromosomeMessage
						.setMessage(e.getMessage()
								+ " (see <a href=\"/start.html#!pages/help\">Help</a>).");
				return false;

			}

		}

		if (validVcfFiles.size() > 0) {

			analyzeMessage.setType(Message.OK);
			analyzeMessage.setMessage(validVcfFiles.size()
					+ " valid VCF file(s) found.");
			chromosomeMessage.setType(Message.OK);

			// init counteres
			context.incCounter("samples", noSamples);
			context.incCounter("genotypes", noSamples * noSnps);
			context.incCounter("chromosomes", noSamples * chromosomes.size());
			context.incCounter("runs", 1);

			return true;

		} else {

			analyzeMessage.setType(Message.ERROR);
			analyzeMessage
					.setMessage("The provided files are not VCF files  (see <a href=\"/start.html#!pages/help\">Help</a>).");
			chromosomeMessage.setType(Message.ERROR);

			return false;
		}
	}

	private boolean importVcfFiles(CloudgeneContext context) {

		for (String input : context.getInputs()) {

			if (ImporterFactory.needsImport(context.get(input))) {

				Message message = createLogMessage("", Message.RUNNING);

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

						message.setMessage("Import " + url2 + "...");

						IImporter importer = ImporterFactory.createImporter(
								url, target);

						if (importer != null) {

							boolean successful = importer.importFiles("vcf.gz");

							if (successful) {

								context.setInput(input, target);

							} else {

								message.setMessage("Import " + url2
										+ " failed: "
										+ importer.getErrorMessage());
								message.setType(Message.ERROR);

								return false;

							}

						} else {

							message.setMessage("Import " + url2
									+ " failed: Protocol not supported");
							message.setType(Message.ERROR);

							return false;

						}

					} catch (Exception e) {
						message.setMessage("Import File(s) " + url2
								+ " failed: " + e.toString());
						message.setType(Message.ERROR);

						return false;
					}

				}

				message.setMessage("File Import successful. ");
				message.setType(Message.OK);

			}

		}

		return true;

	}

}
