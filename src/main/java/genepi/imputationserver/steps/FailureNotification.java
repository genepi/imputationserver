package genepi.imputationserver.steps;

import java.io.File;

import genepi.hadoop.PreferenceStore;
import genepi.hadoop.common.WorkflowContext;
import genepi.hadoop.common.WorkflowStep;
import genepi.io.FileUtil;

public class FailureNotification extends WorkflowStep {

	@Override
	public boolean run(WorkflowContext context) {

		Object mail = context.getData("cloudgene.user.mail");
		Object name = context.getData("cloudgene.user.name");
		String step = context.getData("cloudgene.failedStep.classname").toString();

		// read config if mails should be sent
		String folder = getFolder(FailureNotification.class);
		PreferenceStore store = new PreferenceStore(new File(FileUtil.path(folder, "job.config")));

		String notification = "no";
		if (store.getString("minimac.sendmail") != null && !store.getString("minimac.sendmail").equals("")) {
			notification = store.getString("minimac.sendmail");
		}
		
		String errMail = store.getString("minimac.sendmail.error");

		if (step == null) {
			context.println("No error message sent. Object is empty");
			return true;
		}

		if (notification.equals("yes")) {
			if (mail != null) {

				String subject = "Job " + context.getJobName() + " failed.";
				String message = "Dear " + name + ",\n" + "unfortunately, your job failed. "
						+ "\n\nMore details about the error can be found on https://imputationserver.sph.umich.edu/start.html#!jobs/"
						+ context.getJobName();

				try {
					context.sendMail(subject, message);

					if (!step.equals(InputValidation.class.getName())) {

						// send all errors after input validation to us

						if (errMail != null) {
							for (String mailAdress : errMail.split(",")) {
								context.sendMail(mailAdress, subject + " [" + step + "]", message);
							}
						}
					}

					context.ok("We have sent an email to <b>" + mail + "</b> with the error message.");
					context.println("We have sent an email to <b>" + mail + "</b> with the error message.");
					return true;
				} catch (Exception e) {
					context.error("Sending error message failed: " + e.getMessage());
					context.println("Sending error message failed: " + e.getMessage());
					return false;
				}

			} else {
				context.error(
						"Sending error message failed: no mail address found. Please enter your email address (Account -> Profile).");
				context.println(
						"Sending error message failed: no mail address found. Please enter your email address (Account -> Profile).");
				return false;
			}

		} else {
			context.error("No action required. Email notification has been disabled in job.config");
			context.println("No action required. Email notification has been disabled in job.config");
			return false;
		}
	}
}
