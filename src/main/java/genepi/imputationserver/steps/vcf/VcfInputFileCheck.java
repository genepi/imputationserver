package genepi.minicloudmac.hadoop.validation;

import genepi.hadoop.HdfsUtil;
import genepi.io.FileUtil;
import genepi.minicloudmac.hadoop.validation.io.vcf.VcfPair;
import genepi.minicloudmac.hadoop.validation.io.vcf.VcfPreprocessor;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import cloudgene.mapred.jobs.CloudgeneContext;
import cloudgene.mapred.jobs.CloudgeneStep;
import cloudgene.mapred.jobs.Message;
import cloudgene.mapred.steps.importer.IImporter;
import cloudgene.mapred.steps.importer.ImporterFactory;
import cloudgene.mapred.wdl.WdlStep;

public class VcfInputFileCheck extends CloudgeneStep {

	public static Set<String> validChromosomes = new HashSet<String>();

	static {

		validChromosomes.add("1");
		validChromosomes.add("2");
		validChromosomes.add("3");
		validChromosomes.add("4");
		validChromosomes.add("5");
		validChromosomes.add("6");
		validChromosomes.add("7");
		validChromosomes.add("8");
		validChromosomes.add("9");
		validChromosomes.add("10");
		validChromosomes.add("11");
		validChromosomes.add("12");
		validChromosomes.add("13");
		validChromosomes.add("14");
		validChromosomes.add("15");
		validChromosomes.add("16");
		validChromosomes.add("17");
		validChromosomes.add("18");
		validChromosomes.add("19");
		validChromosomes.add("20");
		validChromosomes.add("21");
		validChromosomes.add("22");
	}

	@Override
	public boolean run(WdlStep step, CloudgeneContext context) {

		for (String input : context.getInputs()) {

			if (ImporterFactory.needsImport(context.get(input))) {

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

						beginTask("Import File(s) " + url2 + "...");

						IImporter importer = ImporterFactory.createImporter(
								url, target);

						if (importer != null) {

							boolean successful = importer.importFiles();

							if (successful) {

								context.setInput(input, target);

								endTask("Import File(s) " + url2
										+ " successful.", Message.OK);

							} else {

								endTask("Import File(s) " + url2 + " failed: "
										+ importer.getErrorMessage(),
										Message.ERROR);

								return false;

							}

						} else {

							endTask("Import File(s) " + url2
									+ " failed: Protocol not supported",
									Message.ERROR);

							return false;

						}

					} catch (Exception e) {
						endTask("Import File(s) " + url2 + " failed: "
								+ e.toString(), Message.ERROR);
						return false;
					}

				}

			}
		}

		String folder = getFolder(VcfInputFileSplitter.class);

		String inputFiles = context.get("files");
		int chunkSize = Integer.parseInt(context.get("chunksize"));

		String files = FileUtil.path(context.getLocalTemp(), "input");

		// exports files from hdfs
		try {

			HdfsUtil.getFolder(inputFiles, files);

		} catch (Exception e) {
			error("Downloading files: " + e.getMessage());
			return false;

		}

		List<VcfPair> finalPairs = new Vector<VcfPair>();

		Message analyzeMessage = createLogMessage("Analyze file ",
				Message.RUNNING);
		List<String> chromosomes = new Vector<String>();

		int chunks = 0;
		int pairId = 0;
		int noSnps = 0;
		int noSamples = 0;
		int noChromosomes = 0;

		boolean phased = true;

		VcfPreprocessor preprocessor = new VcfPreprocessor(chunkSize,
				FileUtil.path(folder, "bin", "tabix"));
		String[] vcfFiles = FileUtil.getFiles(files, "*.vcf.gz$|*.vcf$");

		if (vcfFiles.length == 0) {
			analyzeMessage.setType(Message.ERROR);
			analyzeMessage.setMessage("No valid vcf files found.");
			return false;
		}

		Message chromosomeMessage = createLogMessage("", Message.OK);

		for (String filename : vcfFiles) {
			analyzeMessage.setMessage("Analyze file "
					+ FileUtil.getFilename(filename) + "...");
			VcfPair pair = preprocessor.validate(filename);
			if (pair == null) {
				analyzeMessage.setType(Message.ERROR);
				chromosomeMessage.setType(Message.ERROR);
				chromosomeMessage.setMessage(preprocessor.getError());
				return false;
			} else {

				if (validChromosomes.contains(pair.getChromosome())) {

					finalPairs.add(pair);
					chromosomes.add(pair.getChromosome());
					String chromosomeString = "";
					for (String chr : chromosomes) {
						chromosomeString += " " + chr;
						noChromosomes++;
					}
					noSamples = pair.getNoSamples();
					noSnps += pair.getNoSnps();
					chunks += pair.getChunks().size();

					phased = phased && pair.isPhased();

					chromosomeMessage.setMessage("Samples: " + noSamples + "\n"
							+ "Chromosomes:" + chromosomeString + "\n"
							+ "SNPs: " + noSnps + "\n" + "Chunks: " + chunks
							+ "\n" + "Datatype: "
							+ (phased ? "phased" : "unphased") + "\n"
							+ "Genome Build: 37");

				}
			}

			// TODO: reference panel
			context.incCounter("samples", noSamples);
			context.incCounter("genotypes", noSamples * noSnps);
			context.incCounter("chromosomes", noSamples * noChromosomes);
			context.incCounter("runs", 1);
		}

		if (finalPairs.size() > 0) {

			analyzeMessage.setType(Message.OK);
			analyzeMessage.setMessage(finalPairs.size()
					+ " valid VCF file(s) found.");
			chromosomeMessage.setType(Message.OK);

			return true;

		} else {

			analyzeMessage.setType(Message.ERROR);
			analyzeMessage.setMessage(finalPairs.size()
					+ " valid VCF file(s) found.");
			chromosomeMessage.setType(Message.ERROR);

			return false;
		}

	}

}
