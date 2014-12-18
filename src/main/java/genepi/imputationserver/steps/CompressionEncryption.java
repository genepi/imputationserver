package genepi.imputationserver.steps;

import genepi.hadoop.HdfsUtil;
import genepi.hadoop.common.WorkflowContext;
import genepi.hadoop.common.WorkflowStep;
import genepi.imputationserver.steps.vcf.MergedVcfFile;
import genepi.imputationserver.steps.vcf.VcfFileUtil;
import genepi.io.FileUtil;

import java.io.File;
import java.io.FileInputStream;
import java.util.Collections;
import java.util.List;
import java.util.Vector;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.model.ZipParameters;
import net.lingala.zip4j.util.Zip4jConstants;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import cloudgene.mapred.jobs.Message;

public class CompressionEncryption extends WorkflowStep {

	@Override
	public boolean run(WorkflowContext context) {

		String output = context.get("outputimputation");
		String localOutput = context.get("local");
		String encryption = context.get("encryption");

		String password = RandomStringUtils.randomAlphabetic(10);

		try {

			context.beginTask("Export data...");

			List<String> folders = HdfsUtil.getDirectories(output);

			FileUtil.createDirectory(FileUtil.path(localOutput, "results"));

			// export all chromosomes

			for (String folder : folders) {

				String name = FileUtil.getFilename(folder);

				context.println("Export and merge file " + folder);

				// merge all info files
				HdfsUtil.mergeAndGz(
						FileUtil.path(localOutput, "results", "chr" + name
								+ ".info.gz"), folder, true, ".info");


				Configuration conf = new Configuration();

				FileSystem fileSystem = FileSystem.get(conf);
				Path pathFolder = new Path(folder);
				FileStatus[] files = fileSystem.listStatus(pathFolder);

				List<String> filenames = new Vector<String>();

				// filters by extension and sorts by filename
				for (FileStatus file : files) {
					if (!file.isDir()
							&& !file.getPath().getName().startsWith("_")
							&& file.getPath().getName()
									.endsWith(".dose.vcf.gz")) {
						filenames.add(file.getPath().toString());
					}
				}
				Collections.sort(filenames);

				String vcfOutput = FileUtil.path(localOutput, "results", "chr"
						+ name + ".dose.vcf.gz");
				
				MergedVcfFile vcfFile = new MergedVcfFile(vcfOutput);

				for (String file : filenames) {
					context.println("Read file " + file);
					vcfFile.addFile(fileSystem.open(new Path(file)));
				}
				vcfFile.close();

			}

			// delete temporary files
			HdfsUtil.delete(output);

			context.endTask("Exported data.", Message.OK);

		} catch (Exception e) {
			e.printStackTrace();
			context.endTask("Data compression failed: " + e.getMessage(),
					Message.ERROR);
			return false;
		}

		try {
			context.println("Start zip process...");
			

			context.beginTask("Compress data");

			if (!new File(FileUtil.path(localOutput, "results")).exists()) {
				context.endTask("no results found.", Message.ERROR);
				return false;
			}

			ZipParameters param = new ZipParameters();
			if (encryption.equals("yes")) {
				param.setEncryptFiles(true);
				param.setPassword(password);
				param.setEncryptionMethod(Zip4jConstants.ENC_METHOD_STANDARD);
			}

			ZipFile file = new ZipFile(new File(FileUtil.path(localOutput,
					"results.zip")));
			file.createZipFileFromFolder(FileUtil.path(localOutput, "results"),
					param, false, 0);
			FileUtil.deleteDirectory(FileUtil.path(localOutput, "results"));

			context.endTask("Data compression successful.", Message.OK);

		} catch (Exception e) {
			e.printStackTrace();
			context.endTask("Data compression failed: " + e.getMessage(),
					Message.ERROR);
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

		// send email

		if (encryption.equals("yes")) {

			Object mail = context.getData("cloudgene.user.mail");
			Object name = context.getData("cloudgene.user.name");

			if (mail != null) {

				String subject = "Job " + context.getJobName()
						+ " is complete.";
				String message = "Dear "
						+ name
						+ ",\nthe password for the imputation results is: "
						+ password
						+ "\n\nThe results can be downloaded from https://imputationserver.sph.umich.edu/start.html#!jobs/"
						+ context.getJobName() + "/results";

				try {
					context.sendMail(subject, message);
					context.ok("We have sent an email to <b>" + mail
							+ "</b> with the password.");
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

			return true;
		}

	}

}
