package genepi.imputationserver.steps;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import cloudgene.sdk.internal.IExternalWorkspace;
import cloudgene.sdk.internal.WorkflowContext;
import cloudgene.sdk.internal.WorkflowStep;
import genepi.hadoop.HdfsUtil;
import genepi.hadoop.command.Command;
import genepi.imputationserver.steps.vcf.MergedVcfFile;
import genepi.imputationserver.util.DefaultPreferenceStore;
import genepi.imputationserver.util.ExportObject;
import genepi.imputationserver.util.FileMerger;
import genepi.imputationserver.util.PasswordCreator;
import genepi.io.FileUtil;
import genepi.io.text.LineReader;
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
		String localOutput = context.get("local");
		String aesEncryption = context.get("aesEncryption");
		String mode = context.get("mode");
		String password = context.get("password");

		boolean phasingOnly = false;
		if (mode != null && mode.equals("phasing")) {
			phasingOnly = true;
		}

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

			Map<String, ExportObject> chromosomes = new HashMap<String, ExportObject>();

			for (String folder : folders) {

				String name = FileUtil.getFilename(folder);

				context.println("Prepare files for chromosome " + name);

				List<String> data = new Vector<String>();
				List<String> header = new Vector<String>();
				List<String> info = new Vector<String>();

				header = findFiles(folder, ".header.dose.vcf.gz");

				if (phasingOnly) {
					data = findFiles(folder, ".phased.vcf.gz");
				} else {
					data = findFiles(folder, ".data.dose.vcf.gz");
					info = findFiles(folder, ".info");
				}

				// combine all X. to one folder
				if (name.startsWith("X.")) {
					name = "X";
				}

				ExportObject export = chromosomes.get(name);

				if (export == null) {
					export = new ExportObject();
				}

				ArrayList<String> currentDataList = export.getDataFiles();
				currentDataList.addAll(data);
				export.setDataFiles(currentDataList);

				ArrayList<String> currentHeaderList = export.getHeaderFiles();
				currentHeaderList.addAll(header);
				export.setHeaderFiles(currentHeaderList);

				ArrayList<String> currentInfoList = export.getInfoFiles();
				currentInfoList.addAll(info);
				export.setInfoFiles(currentInfoList);

				chromosomes.put(name, export);

			}

			Set<String> chromosomesSet = chromosomes.keySet();
			boolean lastChromosome = false;
			int index = 0;

			for (String name : chromosomesSet) {

				index++;

				if (index == chromosomesSet.size()) {
					lastChromosome = true;
				}

				ExportObject entry = chromosomes.get(name);

				context.println("Export and merge chromosome " + name);

				// resort for chrX only
				if (name.equals("X")) {
					Collections.sort(entry.getDataFiles(), new ChrXComparator());
					Collections.sort(entry.getInfoFiles(), new ChrXComparator());
				}

				// create temp fir
				String temp = FileUtil.path(localOutput, "temp");
				FileUtil.createDirectory(temp);

				// output files
				String dosageOutput;
				String infoOutput = "";

				if (phasingOnly) {
					dosageOutput = FileUtil.path(temp, "chr" + name + ".phased.vcf.gz");
				} else {
					dosageOutput = FileUtil.path(temp, "chr" + name + ".dose.vcf.gz");
					infoOutput = FileUtil.path(temp, "chr" + name + ".info.gz");
					FileMerger.mergeAndGzInfo(entry.getInfoFiles(), infoOutput);
				}

				MergedVcfFile vcfFile = new MergedVcfFile(dosageOutput);

				// simple header check
				String headerLine = null;
				for (String file : entry.getHeaderFiles()) {

					context.println("Read header file " + file);
					LineReader reader = null;
					try {
						reader = new LineReader(HdfsUtil.open(file));
						while (reader.next()) {
							String line = reader.get();
							if (line.startsWith("#CHROM")) {
								if (headerLine != null) {
									if (headerLine.equals(line)) {
										context.println("  Header is the same as header of first file.");
									} else {
										context.println("  ERROR: Header is different as header of first file.");
										context.println(headerLine);
										context.println(line);
										throw new Exception("Different sample order in chunks.");
									}
								} else {
									headerLine = line;
									vcfFile.addFile(HdfsUtil.open(file));
									context.println("  Keep this header as first header.");
								}
							}

						}
						if (reader != null) {
							reader.close();
						}
						HdfsUtil.delete(file);
					} catch (Exception e) {
						if (reader != null) {
							reader.close();
						}
						StringWriter errors = new StringWriter();
						e.printStackTrace(new PrintWriter(errors));
						context.println("Error reading header file: " + errors.toString());
					}
				}

				if (headerLine == null || headerLine.trim().isEmpty()) {
					throw new Exception("No valid header file found");
				}

				// add data files
				for (String file : entry.getDataFiles()) {
					context.println("Read file " + file);
					vcfFile.addFile(HdfsUtil.open(file));
					HdfsUtil.delete(file);
				}

				vcfFile.close();

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

				ZipParameters param = new ZipParameters();
				param.setEncryptFiles(true);
				param.setEncryptionMethod(EncryptionMethod.ZIP_STANDARD);

				if (aesEncryption != null && aesEncryption.equals("yes")) {
					param.setEncryptionMethod(EncryptionMethod.AES);
					param.setAesKeyStrength(AesKeyStrength.KEY_STRENGTH_256);
					param.setCompressionMethod(CompressionMethod.DEFLATE);
					param.setCompressionLevel(CompressionLevel.NORMAL);
				}

				// create zip file
				ArrayList<File> files = new ArrayList<File>();
				files.add(new File(dosageOutput));

				if (!phasingOnly) {
					files.add(new File(infoOutput));
				}

				String fileName = "chr_" + name + ".zip";
				String filePath = FileUtil.path(localOutput, fileName);
				ZipFile file = new ZipFile(new File(filePath), password.toCharArray());
				file.addFiles(files, param);

				// delete temp dir
				FileUtil.deleteDirectory(temp);

				IExternalWorkspace externalWorkspace = context.getExternalWorkspace();

				if (externalWorkspace != null) {

					long start = System.currentTimeMillis();

					context.log("External Workspace '" + externalWorkspace.getName() + "' found");

					context.log("Start file upload: " + filePath);
					
					String url = externalWorkspace.upload("local", file.getFile());

					long end = (System.currentTimeMillis() - start) / 1000;

					context.log("Upload finished in  " + end + " sec. File Location: " + url);

					context.log("Add " + localOutput + " to custom download");

					String size = FileUtils.byteCountToDisplaySize(file.getFile().length());
					
					context.addDownload("local", fileName, size, url);

					FileUtil.deleteFile(filePath);

					context.log("File deleted: " + filePath);

				} else {
					context.log("No external Workspace set.");
				}
			}

			// delete temporary files
			HdfsUtil.delete(output);

			context.endTask("Exported data.", WorkflowContext.OK);

		} catch (Exception e) {
			e.printStackTrace();
			context.endTask("Data compression failed: " + e.getMessage(), WorkflowContext.ERROR);
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

	private List<String> findFiles(String folder, String pattern) throws IOException {

		Configuration conf = HdfsUtil.getConfiguration();

		FileSystem fileSystem = FileSystem.get(conf);
		Path pathFolder = new Path(folder);
		FileStatus[] files = fileSystem.listStatus(pathFolder);

		List<String> dataFiles = new Vector<String>();
		for (FileStatus file : files) {
			if (!file.isDir() && !file.getPath().getName().startsWith("_")
					&& file.getPath().getName().endsWith(pattern)) {
				dataFiles.add(file.getPath().toString());
			}
		}
		Collections.sort(dataFiles);
		return dataFiles;
	}

	class ChrXComparator implements Comparator<String> {

		List<String> definedOrder = Arrays.asList("X.PAR1", "X.nonPAR", "X.PAR2");

		@Override
		public int compare(String o1, String o2) {

			String region = o1.substring(o1.lastIndexOf("/") + 1).split("_")[1];

			String region2 = o2.substring(o2.lastIndexOf("/") + 1).split("_")[1];

			return Integer.valueOf(definedOrder.indexOf(region))
					.compareTo(Integer.valueOf(definedOrder.indexOf(region2)));
		}

	}

}
