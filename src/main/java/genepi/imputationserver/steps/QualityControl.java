package genepi.imputationserver.steps;

import java.text.DecimalFormat;

import genepi.imputationserver.steps.qc.QualityControlJob;
import genepi.imputationserver.util.HadoopJobStep;
import genepi.imputationserver.util.RefPanel;
import genepi.imputationserver.util.RefPanelList;
import genepi.io.FileUtil;
import cloudgene.mapred.jobs.CloudgeneContext;
import cloudgene.mapred.jobs.Message;
import cloudgene.mapred.wdl.WdlStep;

public class QualityControl extends HadoopJobStep {

	@Override
	public boolean run(WdlStep step, CloudgeneContext context) {

		String folder = getFolder(HadoopJobStep.class);

		// inputs
		String input = context.get("chunkfile");
		String reference = context.get("refpanel");
		String population = context.get("population");

		// outputs
		String output = context.get("outputmaf");
		String outputManifest = context.get("mafchunkfile");

		RefPanelList panels = null;
		try {
			panels = RefPanelList.loadFromFile(FileUtil.path(folder,
					"panels.txt"));

		} catch (Exception e) {

			context.println("panels.txt not found.");
			return false;
		}
		RefPanel panel = panels.getById(reference);
		if (panel == null) {
			context.println("Reference '" + reference + "' not found.");
			context.println("Available references:");
			for (RefPanel p : panels.getPanels()) {
				context.println(p.getId());
			}

			return false;
		}

		QualityControlJob job = new QualityControlJob("maf");
		// job.setJar(FileUtil.path(folder, step.getJar()));
		// job.setJarByClass(QualityControlStep.class);
		job.setLegendHdfs(panel.getLegend());
		job.setLegendPattern(panel.getLegendPattern());
		job.setPopulation(population);
		job.setInput(input);
		job.setOutput(output + "_temp");
		job.setOutputMaf(output);
		job.setOutputManifest(outputManifest);

		boolean successful = executeHadoopJob(job, context);

		if (successful) {
			// print statistics (TODO: remove qstat file!)

			DecimalFormat df = new DecimalFormat("#.00");

			StringBuffer text = new StringBuffer();
			text.append("Duplicated sites: " + job.getDuplicates() + "<br>");
			text.append("NonSNP sites: " + job.getNoSnps() + "<br>");
			text.append("Alternative allele frequency > 0.5 sites: "
					+ job.getAlternativeAlleles() + "<br>");
			text.append("Monomorphic sites: " + job.getMonomorphic() + "<br>");
			text.append("Reference Overlap: "
					+ df.format(job.getFoundInLegend()
							/ (double) (job.getFoundInLegend() + job
									.getNotFoundInLegend()) * 100) + "% "
					+ "<br>");
			text.append("Allele mismatch: " + job.getAlleleMismatch() + "<br>");
			text.append("Excluded SNPs with a call rate of < 90%: "
					+ job.getToLessSamples() + "<br>");
			text.append("Excluded sites in total: " + job.getFiltered()
					+ "<br>");
			text.append("Excluded chunks: " + job.getRemovedChunks());

			ok(text.toString());

			return true;
		} else {
			error("QC Quality Control failed!");
			return false;
		}
	}

}
