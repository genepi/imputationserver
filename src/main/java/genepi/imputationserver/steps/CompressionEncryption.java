package genepi.imputationserver.steps;

import genepi.io.FileUtil;

import java.io.File;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.model.ZipParameters;
import net.lingala.zip4j.util.Zip4jConstants;

import org.apache.commons.lang.RandomStringUtils;

import cloudgene.mapred.jobs.CloudgeneContext;
import cloudgene.mapred.steps.Hadoop;
import cloudgene.mapred.wdl.WdlStep;

public class CompressionEncryption extends Hadoop {

	@Override
	public boolean run(WdlStep step, CloudgeneContext context) {

		try {
		// inputs
		String folder = context.get("local");
		String encryption = context.get("encryption");

		if (!new File(FileUtil.path(folder, "results")).exists()) {
			error("no results found.");
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

			ok("Data compression successful.");

			// submit counters!
			context.submitCounter("samples");
			context.submitCounter("genotypes");
			context.submitCounter("chromosomes");
			context.submitCounter("runs");

			if (encryption.equals("yes")) {

				if (context.getUser().getMail() != null) {

					ok("We have sent an email to <b>"
							+ context.getUser().getMail()
							+ "</b> with the password.");

					String subject = "Job " + context.getJob().getName()
							+ " is complete.";
					String message = "Dear "
							+ context.getUser().getFullName()
							+ ",\nthe password for the imputation results is: "
							+ password
							+ "\n\nThe results can be downloaded from https://imputationserver.sph.umich.edu/start.html#!jobs/"
							+ context.getJob().getName()+"/results";

					return context.sendMail(subject, message);

				} else {
					error("No email address found. Please enter your email address (Account -> Profile).");
					return false;
				}

			} else {

				return true;
			}

		} catch (Exception e) {
			e.printStackTrace();
			error("Data compression failed: " + e.getMessage());
			return false;
		}

	}

}
