package genepi.imputationserver.steps;

import genepi.hadoop.common.WorkflowContext;
import genepi.hadoop.common.WorkflowStep;
import genepi.io.FileUtil;

import java.io.File;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.model.ZipParameters;
import net.lingala.zip4j.util.Zip4jConstants;

import org.apache.commons.lang.RandomStringUtils;

public class CompressionEncryption extends WorkflowStep {

	@Override
	public boolean run(WorkflowContext context) {

		try {
			// inputs
			String folder = context.get("local");
			String encryption = context.get("encryption");

			if (!new File(FileUtil.path(folder, "results")).exists()) {
				context.error("no results found.");
				return true;
			}

			String password = RandomStringUtils.randomAlphabetic(10);

			ZipParameters param = new ZipParameters();
			if (encryption.equals("yes")) {
				param.setEncryptFiles(true);
				param.setPassword(password);
				param.setEncryptionMethod(Zip4jConstants.ENC_METHOD_STANDARD);
			}

			ZipFile file = new ZipFile(new File(FileUtil.path(folder,
					"results.zip")));
			file.createZipFileFromFolder(FileUtil.path(folder, "results"),
					param, false, 0);
			FileUtil.deleteDirectory(FileUtil.path(folder, "results"));

			context.ok("Data compression successful.");

			// submit counters!
			context.submitCounter("samples");
			context.submitCounter("genotypes");
			context.submitCounter("chromosomes");
			context.submitCounter("runs");

			if (encryption.equals("yes")) {

				Object mail = context.getData("cloudgene.user.mail");
				Object name = context.getData("cloudgene.user.name");

				if (mail != null) {

					context.ok("We have sent an email to <b>" + mail
							+ "</b> with the password.");

					String subject = "Job " + context.getJobName()
							+ " is complete.";
					String message = "Dear "
							+ name
							+ ",\nthe password for the imputation results is: "
							+ password
							+ "\n\nThe results can be downloaded from https://imputationserver.sph.umich.edu/start.html#!jobs/"
							+ context.getJobName() + "/results";

					return context.sendMail(subject, message);

				} else {
					context.error("No email address found. Please enter your email address (Account -> Profile).");
					return false;
				}

			} else {

				return true;
			}

		} catch (Exception e) {
			e.printStackTrace();
			context.error("Data compression failed: " + e.getMessage());
			return false;
		}

	}

}
