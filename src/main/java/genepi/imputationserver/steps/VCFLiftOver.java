package genepi.imputationserver.steps;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;

import genepi.hadoop.common.WorkflowContext;
import genepi.hadoop.common.WorkflowStep;
import genepi.imputationserver.steps.fastqc.TaskResults;
import genepi.imputationserver.steps.vcf.VcfLiftOver;
import genepi.io.FileUtil;

public class VCFLiftOver extends WorkflowStep {

	@Override
	public boolean run(WorkflowContext context) {

		String workingDirectory = getFolder(VcfLiftOver.class);
		
		String inputFiles = context.get("files");
		String chain = context.get("chain");
		String outputFolder = context.get("output");


		try {
			context.beginTask("Analyze files");

			String[] vcfFilenames = FileUtil.getFiles(inputFiles, "*.vcf.gz$|*.vcf$");
			Arrays.sort(vcfFilenames);

			for (String input : vcfFilenames) {
				String name = FileUtil.getFilename(input);
				String output = FileUtil.path(outputFolder, name);
				context.updateTask("Analyze file " + name + "...", WorkflowContext.RUNNING);

				VcfLiftOver.liftOver(input, output, FileUtil.path(workingDirectory, chain), context.getLocalTemp());
			}
			context.endTask(vcfFilenames.length + " VCF file(s) analyzed.", WorkflowContext.OK);

			return true;

		} catch (Exception e) {
			e.printStackTrace();
			TaskResults result = new TaskResults();
			result.setSuccess(false);
			result.setMessage(e.getMessage());
			StringWriter s = new StringWriter();
			e.printStackTrace(new PrintWriter(s));
			context.println("VCFLiftOver failed.\nException:" + s.toString());
			context.endTask("VCFLiftOver failed." + "' failed.\nException:" + s.toString(), WorkflowContext.ERROR);
			return false;
		} catch (Error e) {
			e.printStackTrace();
			TaskResults result = new TaskResults();
			result.setSuccess(false);
			result.setMessage(e.getMessage());
			StringWriter s = new StringWriter();
			e.printStackTrace(new PrintWriter(s));
			context.println("VCFLiftOver failed.\nException:" + s.toString());
			context.endTask("VCFLiftOver failed." + "' failed.\nException:" + s.toString(), WorkflowContext.ERROR);
			return false;
		}

	}

}
