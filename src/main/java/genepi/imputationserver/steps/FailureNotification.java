package genepi.imputationserver.steps;

import cloudgene.mapred.jobs.CloudgeneContext;
import cloudgene.mapred.util.MailUtil;
import cloudgene.mapred.util.Settings;
import cloudgene.mapred.wdl.WdlStep;
import genepi.hadoop.common.WorkflowContext;
import genepi.hadoop.common.WorkflowStep;

public class FailureNotification extends WorkflowStep {

	@Override
	public boolean run(WorkflowContext context) {

		Object mail = context.getData("cloudgene.user.mail");
		Object name = context.getData("cloudgene.user.name");
		Object stepObject = context.getData("cloudgene.failedStep");

		if (stepObject == null) {
			context.println("No error message sent. Object is empty");
			return true;
		}

		WdlStep step = (WdlStep) stepObject;

		if (mail != null) {

			String subject = "Job " + context.getJobName() + " failed.";
			String message = "Dear "
					+ name
					+ ",\n"
					+ "unfortunately, your job failed. "
					+ "\n\nMore details about the error can be found on https://imputationserver.sph.umich.edu/start.html#!jobs/"
					+ context.getJobName();

			try {
				context.sendMail(subject, message);

				if (!step.getClassname()
						.equals(InputValidation.class.getName())) {

					// send all errors after input validation to us

					context.sendMail("sebastian.schoenherr@uibk.ac.at", subject
							+ " [" + step.getClassname() + "]", message);

					context.sendMail("lukas.forer@i-med.ac.at", subject + " ["
							+ step.getClassname() + "]", message);
				}

				context.ok("We have sent an email to <b>" + mail
						+ "</b> with the error message.");
				context.println("We have sent an email to <b>" + mail
						+ "</b> with the error message.");
				return true;
			} catch (Exception e) {
				context.error("Sending error message failed: " + e.getMessage());
				context.println("Sending error message failed: "
						+ e.getMessage());
				return false;
			}

		} else {
			context.error("Sending error message failed: no mail address found. Please enter your email address (Account -> Profile).");
			context.println("Sending error message failed: no mail address found. Please enter your email address (Account -> Profile).");
			return false;
		}

	}

}
