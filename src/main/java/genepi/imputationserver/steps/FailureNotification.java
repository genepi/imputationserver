package genepi.imputationserver.steps;

import java.io.File;

import cloudgene.sdk.internal.WorkflowContext;
import cloudgene.sdk.internal.WorkflowStep;
import genepi.imputationserver.util.DefaultPreferenceStore;
import genepi.io.FileUtil;

public class FailureNotification extends WorkflowStep {

	@Override
	public boolean run(WorkflowContext context) {

		Object mail = context.getData("cloudgene.user.mail");
		Object name = context.getData("cloudgene.user.name");
		String step = context.getData("cloudgene.failedStep.classname").toString();

		// load job.config
		String folder = getFolder(FailureNotification.class);
		File jobConfig = new File(FileUtil.path(folder, "job.config"));
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

		if (step == null) {
			context.println("No error message sent. Object is empty");
			return true;
		}

		if (notification.equals("yes")) {

			if (!step.equals(InputValidation.class.getName())) {

				// send all errors after input validation to slack
				try {
					context.sendNotification("Job *" + context.getJobId() + "* failed in *" + step
							+ "* :thinking_face:\n" + serverUrl + "/start.html#!jobs/" + context.getJobId());
				} catch (Exception e) {
					context.println("Sending notification message failed: " + e.getMessage());
				}
			}

			if (mail != null) {

				String subject = "Job " + context.getJobId() + " failed.";
				String message = "Dear " + name + ",\n" + "unfortunately, your job failed. "
						+ "\n\nMore details about the error can be found on " + serverUrl + "/start.html#!jobs/"
						+ context.getJobId();

				try {
					context.sendMail(subject, message);

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

		} else

		{
			context.warning("No action required. Email notification has been disabled in job.config");
			context.println("No action required. Email notification has been disabled in job.config");
			return false;
		}
	}
}
