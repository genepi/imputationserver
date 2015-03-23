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
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.Vector;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

import cloudgene.mapred.steps.importer.IImporter;
import cloudgene.mapred.steps.importer.ImporterFactory;

public class InputValidation extends WorkflowStep {

	@Override
	public boolean run(WorkflowContext context) {

		URLClassLoader cl = (URLClassLoader) InputValidation.class
				.getClassLoader();
		try {
			URL url = cl.findResource("META-INF/MANIFEST.MF");
			Manifest manifest = new Manifest(url.openStream());
			Attributes attr = manifest.getMainAttributes();
			String buildVesion = attr.getValue("Version");
			String buildTime = attr.getValue("Build-Time");
			String builtBy = attr.getValue("Built-By");
			context.println("Version: " + buildVesion + " (Built by " + builtBy
					+ " on " + buildTime + ")");

		} catch (IOException E) {
			// handle
		}

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
		String population = context.get("population");
		String phasing = context.get("phasing");

		int chunkSize = Integer.parseInt(context.get("chunksize"));

		VcfFileUtil.setBinaries(FileUtil.path(folder, "bin"));

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

				VcfFile vcfFile = VcfFileUtil.load(filename, chunkSize, true);

				String mail = context.getData("cloudgene.user.mail").toString();
				boolean chrXTester = mail.toLowerCase().equals(
						"narisu@mail.nih.gov")
						|| mail.toLowerCase().equals(
								"sebastian.schoenherr@uibk.ac.at")
						|| mail.toLowerCase().equals("lukas.forer@i-med.ac.at")
						|| mail.toLowerCase().equals("cfuchsb@umich.edu");
				if (chrXTester
						|| VcfFileUtil.isAutosomal(vcfFile.getChromosome())) {

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
										+ vcfFile.getNoSamples()
										+ " vs "
										+ noSamples + ")",
								WorkflowContext.ERROR);
						return false;
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

					if ((panel.getId().equals("hapmap2") && !population
							.equals("eur"))) {

						context.endTask(
								"Please select the EUR population for the HapMap reference panel",
								WorkflowContext.ERROR);

						return false;
					}

					if ((panel.getId().equals("phase1") && population
							.equals("sas"))
							|| (panel.getId().equals("phase1") && population
									.equals("eas"))) {

						context.endTask(
								"The selected population (SAS, EAS) is not allowed for this panel",
								WorkflowContext.ERROR);

						return false;
					}

					if ((panel.getId().equals("phase3") && population
							.equals("asn"))) {

						context.endTask(
								"The selected population (ASN) is not allowed for the 1000G Phase3 reference panel",
								WorkflowContext.ERROR);

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

		if (!phased && noSamples < 50) {
			context.endTask(
					"At least 50 samples must be included for pre-phasing",
					WorkflowContext.ERROR);

			return false;
		}

		if (noSamples > 6000) {
			context.endTask(
					"The maximum allowed number of samples is 6,000. Please contact Christian Fuchsberger (<a href=\"mailto:cfuchsb@umich.edu\">cfuchsb@umich.edu</a>) to discuss this large imputation.",
					WorkflowContext.ERROR);

			return false;
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
			context.incCounter("phasing_" + phasing, 1);

			return true;

		} else {

			context.endTask(
					"The provided files are not VCF files  (see <a href=\"/start.html#!pages/help\">Help</a>). Chromosome X is currently in Beta.",
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
