package genepi.minicloudmac.hadoop.compression;

import java.io.File;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.model.ZipParameters;
import net.lingala.zip4j.util.Zip4jConstants;

import org.apache.commons.lang.RandomStringUtils;

import cloudgene.mapred.jobs.CloudgeneContext;
import cloudgene.mapred.steps.Hadoop;
import cloudgene.mapred.util.FileUtil;
import cloudgene.mapred.wdl.WdlStep;

public class Compression extends Hadoop {

	@Override
	public boolean run(WdlStep step, CloudgeneContext context) {

		String password = RandomStringUtils.randomAlphabetic(10);
		String folder = context.get("local");
		// String report = context.get("report");
		String encryption = context.get("encryption");

		try {

			ZipParameters param = new ZipParameters();
			if (encryption.equals("yes")) {
				param.setEncryptFiles(true);
				param.setPassword(password);
				param.setEncryptionMethod(Zip4jConstants.ENC_METHOD_STANDARD);
			}

			/*
			 * new File(report).renameTo(new File(FileUtil.path(folder,
			 * "results", "report.html")));
			 */

			ZipFile file = new ZipFile(new File(FileUtil.path(folder,
					"results.zip")));
			file.createZipFileFromFolder(FileUtil.path(folder, "results"),
					param, false, 0);
			FileUtil.deleteDirectory(FileUtil.path(folder, "results"));

			ok("Data compression successful.");

			//submit counters!
			context.submitCounter("samples");
			context.submitCounter("genotypes");
			context.submitCounter("chromosomes");
			context.submitCounter("runs");
			
			if (encryption.equals("yes")) {

				ok("We have sent an email to <b>" + context.getUser().getMail()
						+ "</b> with the password.");

				String subject = "Job " + context.getJob().getName()
						+ " is complete.";
				String message = "Dear "
						+ context.getUser().getFullName()
						+ ",\nthe password for the imputation results is: "
						+ password
						+ "\n\nThe results can be downloaded from https://imputationserver.sph.umich.edu/start.html#!jobs/"
						+ context.getJob().getName();

				return context.sendMail(subject, message);

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
