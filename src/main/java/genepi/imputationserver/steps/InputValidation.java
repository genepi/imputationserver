package genepi.imputationserver.steps;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

import genepi.hadoop.PreferenceStore;
import genepi.hadoop.common.WorkflowContext;
import genepi.hadoop.common.WorkflowStep;
import genepi.hadoop.importer.IImporter;
import genepi.hadoop.importer.ImporterFactory;
import genepi.imputationserver.steps.converter.VCFBuilder;
import genepi.imputationserver.steps.vcf.VcfFile;
import genepi.imputationserver.steps.vcf.VcfFileUtil;
import genepi.imputationserver.util.RefPanel;
import genepi.imputationserver.util.RefPanelList;
import genepi.io.FileUtil;

public class InputValidation extends WorkflowStep {

	@Override
	public boolean run(WorkflowContext context) {

		URLClassLoader cl = (URLClassLoader) InputValidation.class.getClassLoader();

		try {
			URL url = cl.findResource("META-INF/MANIFEST.MF");
			Manifest manifest = new Manifest(url.openStream());
			Attributes attr = manifest.getMainAttributes();
			String buildVesion = attr.getValue("Version");
			String buildTime = attr.getValue("Build-Time");
			String builtBy = attr.getValue("Built-By");
			context.println("Version: " + buildVesion + " (Built by " + builtBy + " on " + buildTime + ")");

		} catch (IOException E) {
			// handle
		}

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
		String phasing = context.get("phasing");
		String build = context.get("build");
		String r2Filter = context.get("r2Filter");

		int sampleLimit = Integer.valueOf(context.get("sample-limit"));
		int chunkSize = Integer.parseInt(context.get("chunksize"));

		PreferenceStore store = new PreferenceStore(new File(FileUtil.path(folder, "job.config")));

		List<VcfFile> validVcfFiles = new Vector<VcfFile>();

		context.beginTask("Analyze files ");
		List<String> chromosomes = new Vector<String>();

		int chunks = 0;
		int noSnps = 0;
		int noSamples = 0;

		boolean phased = true;

		try {
			String[] genome = FileUtil.getFiles(files, "*.txt$|*.zip");

			if (genome.length == 1) {
				context.updateTask("Check for valid 23andMe data", WorkflowContext.RUNNING);
				VCFBuilder builder = new VCFBuilder(genome[0]);

				String newFiles = FileUtil.path(context.getLocalTemp(), "vcfs");
				String tempFiles = FileUtil.path(context.getLocalTemp(), "23andMe-temp");

				builder.setReference(store.getString("ref.fasta"));
				builder.setOutDirectory(newFiles);
				builder.setExcludeList("MT,X,Y");
				builder.setTempDirectory(tempFiles);
				builder.build();

				// update files with new location
				files = newFiles;
				context.setInput("inputs", newFiles);

				context.incCounter("23andme-input", 1);
			} else if (genome.length > 1) {
				context.endTask("Please upload your 23andMe data as a single txt or zip file", WorkflowContext.ERROR);
				return false;
			}
		} catch (Exception e1) {
			context.endTask("Converter task failed! \n" + e1.getMessage(), WorkflowContext.ERROR);
			e1.printStackTrace();
			return false;
		}

		String[] vcfFiles = FileUtil.getFiles(files, "*.vcf.gz$|*.vcf$");

		if (vcfFiles.length == 0) {
			context.endTask("The provided files are not VCF files (see <a href=\"/start.html#!pages/help\">Help</a>).",
					WorkflowContext.ERROR);
			return false;
		}

		Arrays.sort(vcfFiles);

		String infos = null;

		RefPanelList panels = RefPanelList.loadFromFile(FileUtil.path(folder, RefPanelList.FILENAME));

		RefPanel panel = panels.getById(reference, context.getData("refpanel"));
		if (panel == null) {
			context.endTask("Reference '" + reference + "' not found.", WorkflowContext.ERROR);
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

				if (VcfFileUtil.isValidChromosome(vcfFile.getChromosome())) {

					if (VcfFileUtil.isChrX(vcfFile.getChromosome())) {

						if (!phasing.equals("eagle") && !vcfFile.isPhased()) {
							context.endTask("Please select eagle2 for chromosome X. ", WorkflowContext.ERROR);
							return false;
						}

					}

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

					if (!phased && noSamples < 50 && !phasing.equals("eagle")) {
						context.endTask("At least 50 samples must be included for pre-phasing using " + phasing
								+ ". Please select eagle.", WorkflowContext.ERROR);

						return false;
					}

					if (noSamples > sampleLimit && sampleLimit != 0) {
						context.endTask(
								"The maximum number of samples is " + sampleLimit
										+ ". Please contact Christian Fuchsberger (<a href=\"mailto:cfuchsb@umich.edu\">cfuchsb@umich.edu</a>) to discuss this large imputation.",
								WorkflowContext.ERROR);

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
							+ "Phasing: " + phasing;

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
			context.incCounter("phasing_" + phasing, 1);

			return true;

		} else {

			context.endTask(
					"The provided files are not VCF files  (see <a href=\"/start.html#!pages/help\">Help</a>). Chromosome X is currently in Beta.",
					WorkflowContext.ERROR);

			return false;
		}
	}

	private boolean checkParameters(WorkflowContext context) {

		String folder = getFolder(InputValidation.class);
		setupTabix(folder);
		String reference = context.get("refpanel");
		String population = context.get("population");
		String phasing = context.get("phasing");
		String mode = context.get("mode");

		RefPanelList panels = RefPanelList.loadFromFile(FileUtil.path(folder, RefPanelList.FILENAME));

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

		// check if mode is imputation
		if (mode == null || mode.equals("imputation")) {

			// check if reference panel supports selected phasing algorithm
			if (phasing.equals("hapiur") && panel.getMapHapiUR() == null) {
				context.error("Reference panel " + reference + " doesn't support phasing with HapiUR.");
				return false;
			}

			if (phasing.equals("shapeit") && panel.getMapShapeIT() == null) {
				context.error("Reference panel " + reference + " doesn't support phasing with ShapeIt.");
				return false;
			}

			if (phasing.equals("eagle") && panel.getMapEagle() == null) {
				context.error("Reference panel " + reference + " doesn't support phasing with Eagle.");
				return false;
			}

		}

		// check populations
		if ((reference.contains("hrc") && !population.equals("eur"))) {

			context.error("Please select the EUR population for the HRC panel");

			return false;
		}

		if ((reference.equals("caapa") && !population.equals("AA"))) {

			context.error("Please select the AA population for the CAAPA panel");

			return false;
		}

		if ((reference.equals("hapmap2") && !population.equals("eur"))) {

			context.error("Please select the EUR population for the HapMap reference panel");

			return false;
		}

		if ((reference.equals("phase1") && population.equals("sas"))
				|| (reference.equals("phase1") && population.equals("eas"))) {

			context.error("The selected population (SAS, EAS) is not allowed for this panel");

			return false;
		}

		if ((reference.equals("phase3") && population.equals("asn"))) {

			context.error("The selected population (ASN) is not allowed for the 1000G Phase3 reference panel");

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
