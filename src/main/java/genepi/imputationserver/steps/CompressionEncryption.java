package genepi.imputationserver.steps;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.commons.io.FileUtils;

import cloudgene.sdk.internal.IExternalWorkspace;
import cloudgene.sdk.internal.WorkflowContext;
import cloudgene.sdk.internal.WorkflowStep;
import genepi.hadoop.HdfsUtil;
import genepi.hadoop.command.Command;
import genepi.imputationserver.steps.imputation.ImputationPipeline;
import genepi.imputationserver.steps.vcf.MergedVcfFile;
import genepi.imputationserver.util.DefaultPreferenceStore;
import genepi.imputationserver.util.FileChecksum;
import genepi.imputationserver.util.FileMerger;
import genepi.imputationserver.util.ImputationResults;
import genepi.imputationserver.util.ImputedChromosome;
import genepi.imputationserver.util.PasswordCreator;
import genepi.imputationserver.util.PgsPanel;
import genepi.io.FileUtil;
import genepi.io.text.LineWriter;
import genepi.riskscore.io.MetaFile;
import genepi.riskscore.io.OutputFile;
import genepi.riskscore.io.ReportFile;
import genepi.riskscore.io.SamplesFile;
import genepi.riskscore.tasks.CreateHtmlReportTask;
import genepi.riskscore.tasks.MergeReportTask;
import genepi.riskscore.tasks.MergeScoreTask;
import lukfor.progress.TaskService;
import lukfor.progress.tasks.Task;
import net.lingala.zip4j.ZipFile;
import net.lingala.zip4j.model.ZipParameters;
import net.lingala.zip4j.model.enums.AesKeyStrength;
import net.lingala.zip4j.model.enums.CompressionLevel;
import net.lingala.zip4j.model.enums.CompressionMethod;
import net.lingala.zip4j.model.enums.EncryptionMethod;

public class CompressionEncryption extends WorkflowStep {

	@Override
	public boolean run(WorkflowContext context) {

		String workingDirectory = getFolder(CompressionEncryption.class);

		String output = context.get("outputimputation");
		String outputScores = context.get("outputScores");
		String localOutput = context.get("local");
		String pgsOutput = context.get("pgs_output");
		String aesEncryptionValue = context.get("aesEncryption");
		String meta = context.get("meta");
		String mode = context.get("mode");
		String password = context.get("password");

		PgsPanel pgsPanel = PgsPanel.loadFromProperties(context.getData("pgsPanel"));

		boolean phasingOnly = false;
		if (mode != null && mode.equals("phasing")) {
			phasingOnly = true;
		}

		boolean mergeMetaFiles = !phasingOnly && (meta != null && meta.equals("yes"));

		boolean aesEncryption = (aesEncryptionValue != null && aesEncryptionValue.equals("yes"));

		// read config if mails should be sent
		String folderConfig = getFolder(CompressionEncryption.class);
		File jobConfig = new File(FileUtil.path(folderConfig, "job.config"));
		DefaultPreferenceStore store = new DefaultPreferenceStore();
		if (jobConfig.exists()) {
			store.load(jobConfig);
		} else {
			context.log("Configuration file '" + jobConfig.getAbsolutePath() + "' not available. Use default values.");
		}

		String notification = "no";
		if (store.getString("minimac.sendmail") != null && !store.getString("minimac.sendmail").equals("")) {
			notification = store.getString("minimac.sendmail");
		}

		String serverUrl = "https://imputationserver.sph.umich.edu";
		if (store.getString("server.url") != null && !store.getString("server.url").isEmpty()) {
			serverUrl = store.getString("server.url");
		}

		String sanityCheck = "yes";
		if (store.getString("sanitycheck") != null && !store.getString("sanitycheck").equals("")) {
			sanityCheck = store.getString("sanitycheck");
		}

		if (password == null || (password != null && password.equals("auto"))) {
			password = PasswordCreator.createPassword();
		}

		try {

			context.beginTask("Export data...");

			// get sorted directories
			List<String> folders = HdfsUtil.getDirectories(output);

			ImputationResults imputationResults = new ImputationResults(folders, phasingOnly);
			Map<String, ImputedChromosome> imputedChromosomes = imputationResults.getChromosomes();

			Set<String> chromosomes = imputedChromosomes.keySet();
			boolean lastChromosome = false;
			int index = 0;

			String checksumFilename = FileUtil.path(localOutput, "results.md5");
			LineWriter writer = new LineWriter(checksumFilename);

			for (String name : chromosomes) {

				index++;

				if (index == chromosomes.size()) {
					lastChromosome = true;
				}

				ImputedChromosome imputedChromosome = imputedChromosomes.get(name);

				context.println("Export and merge chromosome " + name);

				// create temp dir
				String temp = FileUtil.path(localOutput, "temp");
				FileUtil.createDirectory(temp);

				// output files

				ArrayList<File> files = new ArrayList<File>();

				// merge info files
				if (!phasingOnly) {
					String infoOutput = FileUtil.path(temp, "chr" + name + ".info.gz");
					FileMerger.mergeAndGzInfo(imputedChromosome.getInfoFiles(), infoOutput);
					files.add(new File(infoOutput));
				}

				// merge all dosage files

				String dosageOutput;
				if (phasingOnly) {
					dosageOutput = FileUtil.path(temp, "chr" + name + ".phased.vcf.gz");
				} else {
					dosageOutput = FileUtil.path(temp, "chr" + name + ".dose.vcf.gz");
				}
				files.add(new File(dosageOutput));

				MergedVcfFile vcfFile = new MergedVcfFile(dosageOutput);
				vcfFile.addHeader(context, imputedChromosome.getHeaderFiles());

				for (String file : imputedChromosome.getDataFiles()) {
					context.println("Read file " + file);
					vcfFile.addFile(HdfsUtil.open(file));
					HdfsUtil.delete(file);
				}

				vcfFile.close();

				// merge all meta files
				if (mergeMetaFiles) {

					context.println("Merging meta files...");

					String dosageMetaOutput = FileUtil.path(temp, "chr" + name + ".empiricalDose.vcf.gz");
					MergedVcfFile vcfFileMeta = new MergedVcfFile(dosageMetaOutput);

					String headerMetaFile = imputedChromosome.getHeaderMetaFiles().get(0);
					context.println("Use header from file " + headerMetaFile);

					vcfFileMeta.addFile(HdfsUtil.open(headerMetaFile));

					for (String file : imputedChromosome.getDataMetaFiles()) {
						context.println("Read file " + file);
						vcfFileMeta.addFile(HdfsUtil.open(file));
						HdfsUtil.delete(file);
					}
					vcfFileMeta.close();

					context.println("Meta files merged.");

					files.add(new File(dosageMetaOutput));
				}

				if (sanityCheck.equals("yes") && lastChromosome) {
					context.log("Run tabix on chromosome " + name + "...");
					Command tabix = new Command(FileUtil.path(workingDirectory, "bin", "tabix"));
					tabix.setSilent(false);
					tabix.setParams("-f", dosageOutput);
					if (tabix.execute() != 0) {
						context.endTask("Error during index creation: " + tabix.getStdOut(), WorkflowContext.ERROR);
						return false;
					}
					context.log("Tabix done.");
				}

				// create zip file
				String fileName = "chr_" + name + ".zip";
				String filePath = FileUtil.path(localOutput, fileName);
				File file = new File(filePath);
				createEncryptedZipFile(file, files, password, aesEncryption);

				// add checksum to hash file
				context.log("Creating file checksum for " + filePath);
				long checksumStart = System.currentTimeMillis();
				String checksum = FileChecksum.HashFile(new File(filePath), FileChecksum.Algorithm.MD5);
				writer.write(checksum + " " + fileName);
				long checksumEnd = (System.currentTimeMillis() - checksumStart) / 1000;
				context.log("File checksum for " + filePath + " created in " + checksumEnd + " seconds.");

				// delete temp dir
				FileUtil.deleteDirectory(temp);

				IExternalWorkspace externalWorkspace = context.getExternalWorkspace();

				if (externalWorkspace != null) {

					long start = System.currentTimeMillis();

					context.log("External Workspace '" + externalWorkspace.getName() + "' found");

					context.log("Start file upload: " + filePath);

					String url = externalWorkspace.upload("local", file);

					long end = (System.currentTimeMillis() - start) / 1000;

					context.log("Upload finished in  " + end + " sec. File Location: " + url);

					context.log("Add " + localOutput + " to custom download");

					String size = FileUtils.byteCountToDisplaySize(file.length());

					context.addDownload("local", fileName, size, url);

					FileUtil.deleteFile(filePath);

					context.log("File deleted: " + filePath);

				} else {
					context.log("No external Workspace set.");
				}
			}

			writer.close();

			// delete temporary files
			HdfsUtil.delete(output);

			// Export calculated risk scores
			if (pgsPanel != null) {

				context.println("Exporting PGS scores...");

				String scoresFolder = FileUtil.path(context.getLocalTemp(), "scores");
				FileUtil.createDirectory(scoresFolder);

				List<String> scoreList = HdfsUtil.getFiles(outputScores);

				String[] chunksScores = new String[scoreList.size() / 2];
				String[] chunksReports = new String[scoreList.size() / 2];

				int chunksScoresCount = 0;
				int chunksReportsCount = 0;

				for (String score : scoreList) {

					String filename = FileUtil.getFilename(score);
					String localPath = FileUtil.path(scoresFolder, filename);
					HdfsUtil.get(score, localPath);

					if (score.endsWith(".json")) {
						chunksReports[chunksReportsCount] = localPath;
						chunksReportsCount++;
					} else {
						chunksScores[chunksScoresCount] = localPath;
						chunksScoresCount++;
					}

				}

				String outputFileScores = FileUtil.path(context.getLocalTemp(), "scores.txt");
				String outputFileReports = FileUtil.path(context.getLocalTemp(), "report.json");
				String outputFileHtml = FileUtil.path(pgsOutput, "scores.html");

				String extendedHtmlFolder = FileUtil.path(context.getLocalTemp(), "report");
				FileUtil.createDirectory(extendedHtmlFolder);
				String outputFileExtendedHtml = FileUtil.path(extendedHtmlFolder, "scores.html");

				String samples = FileUtil.path(pgsOutput, "samples.txt");

				// disable ansi
				TaskService.setAnsiSupport(false);

				MergeScoreTask mergeScore = new MergeScoreTask();
				mergeScore.setInputs(chunksScores);
				mergeScore.setOutput(outputFileScores);
				TaskService.run(mergeScore);

				String fileName = "scores.zip";
				String filePath = FileUtil.path(pgsOutput, fileName);
				File file = new File(filePath);
				createEncryptedZipFile(file, new File(outputFileScores), password, aesEncryption);

				context.println("Exported PGS scores to " + fileName + ".");

				MergeReportTask mergeReport = new MergeReportTask();
				mergeReport.setInputs(chunksReports);
				mergeReport.setOutput(outputFileReports);
				TaskService.run(mergeReport);

				ReportFile report = mergeReport.getResult();

				String metaFilename = pgsPanel.getMeta() != null ? pgsPanel.getMeta()
						: FileUtil.path(workingDirectory, "pgs-catalog.json");

				if (new File(metaFilename).exists()) {
					context.println("Loading meta file from " + metaFilename + ".");
					MetaFile metaFile = MetaFile.load(metaFilename);
					report.mergeWithMeta(metaFile);
				} else {
					context.println("Warning: Meta file " + metaFilename + " not found.");
				}

				CreateHtmlReportTask htmlReport = createReport(outputFileHtml, outputFileScores, samples, report,
						"default", false);
				CreateHtmlReportTask htmlExtendedReport = createReport(outputFileExtendedHtml, outputFileScores,
						samples, report, "multi-page", true);

				List<Task> runningTasks = TaskService.run(htmlReport, htmlExtendedReport);
				for (Task runningTask : runningTasks) {
					if (!runningTask.getStatus().isSuccess()) {
						context.endTask("Html Report failed: " + runningTask.getStatus().getThrowable(),
								WorkflowContext.ERROR);
						return false;
					}
				}

				String fileNameReport = "scores.report.zip";
				File fileReport = new File(FileUtil.path(pgsOutput, fileNameReport));
				createEncryptedZipFileFromFolder(fileReport, new File(extendedHtmlFolder), password, aesEncryption);

				context.println("Created reports " + outputFileHtml + " and " + fileReport.getPath() + ".");

			}

			context.endTask("Exported data.", WorkflowContext.OK);

		} catch (Exception e) {
			e.printStackTrace();
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			e.printStackTrace(pw);
			context.endTask("Data export failed: " + sw.toString(), WorkflowContext.ERROR);
			return false;
		}

		// submit counters!
		context.submitCounter("samples");
		context.submitCounter("genotypes");
		context.submitCounter("chromosomes");
		context.submitCounter("runs");
		// submit panel and phasing method counters
		String reference = context.get("refpanel");
		String phasing = context.get("phasing");
		context.submitCounter("refpanel_" + reference);
		context.submitCounter("phasing_" + phasing);
		context.submitCounter("23andme-input");

		// send email
		if (notification.equals("yes")) {

			Object mail = context.getData("cloudgene.user.mail");
			Object name = context.getData("cloudgene.user.name");

			if (mail != null) {

				String subject = "Job " + context.getJobId() + " is complete.";
				String message = "Dear " + name + ",\nthe password for the imputation results is: " + password
						+ "\n\nThe results can be downloaded from " + serverUrl + "/start.html#!jobs/"
						+ context.getJobId() + "/results";

				try {
					context.sendMail(subject, message);
					context.ok("We have sent an email to <b>" + mail + "</b> with the password.");
					return true;
				} catch (Exception e) {
					context.error("Data compression failed: " + e.getMessage());
					return false;
				}

			} else {
				context.error("No email address found. Please enter your email address (Account -> Profile).");
				return false;
			}

		} else {
			context.ok(
					"Email notification is disabled. All results are encrypted with password <b>" + password + "</b>");
			return true;
		}

	}

	private CreateHtmlReportTask createReport(String outputFileHtml, String outputFileScores, String samples,
			ReportFile report, String template, boolean showDistribution) throws IOException, Exception {
		CreateHtmlReportTask task = new CreateHtmlReportTask();
		task.setApplicationName("");
		task.setVersion("PGS Server Beta <small>(" + ImputationPipeline.PIPELINE_VERSION + ")</small>");
		task.setShowCommand(false);
		task.setReport(report);
		task.setOutput(outputFileHtml);
		task.setData(new OutputFile(outputFileScores));
		task.setTemplate(template);
		task.setShowDistribution(showDistribution);
		if (new File(samples).exists()) {
			SamplesFile samplesFile = new SamplesFile(samples);
			samplesFile.buildIndex();
			task.setSamples(samplesFile);
		}
		return task;
	}

	public void createEncryptedZipFile(File file, List<File> files, String password, boolean aesEncryption)
			throws IOException {
		ZipParameters param = new ZipParameters();
		param.setEncryptFiles(true);
		param.setEncryptionMethod(EncryptionMethod.ZIP_STANDARD);

		if (aesEncryption) {
			param.setEncryptionMethod(EncryptionMethod.AES);
			param.setAesKeyStrength(AesKeyStrength.KEY_STRENGTH_256);
			param.setCompressionMethod(CompressionMethod.DEFLATE);
			param.setCompressionLevel(CompressionLevel.NORMAL);
		}

		ZipFile zipFile = new ZipFile(file, password.toCharArray());
		zipFile.addFiles(files, param);
		zipFile.close();
	}

	public void createEncryptedZipFile(File file, File source, String password, boolean aesEncryption)
			throws IOException {
		List<File> files = new Vector<File>();
		files.add(source);
		createEncryptedZipFile(file, files, password, aesEncryption);
	}

	public void createEncryptedZipFileFromFolder(File file, File folder, String password, boolean aesEncryption)
			throws IOException {
		ZipParameters param = new ZipParameters();
		param.setEncryptFiles(true);
		param.setEncryptionMethod(EncryptionMethod.ZIP_STANDARD);

		if (aesEncryption) {
			param.setEncryptionMethod(EncryptionMethod.AES);
			param.setAesKeyStrength(AesKeyStrength.KEY_STRENGTH_256);
			param.setCompressionMethod(CompressionMethod.DEFLATE);
			param.setCompressionLevel(CompressionLevel.NORMAL);
		}

		ZipFile zipFile = new ZipFile(file, password.toCharArray());
		zipFile.addFolder(folder);
		zipFile.close();
	}

}
