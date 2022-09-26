package genepi.imputationserver.steps.ancestry;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.gson.Gson;

import genepi.hadoop.HdfsUtil;
import genepi.hadoop.common.WorkflowContext;
import genepi.hadoop.common.WorkflowStep;
import genepi.imputationserver.steps.vcf.VcfFile;
import genepi.imputationserver.steps.vcf.VcfFileUtil;
import genepi.io.FileUtil;

public class TraceBatchGenerator extends WorkflowStep {

	@Override
	public boolean run(WorkflowContext context) {

		String genotypes = context.get("files");
		int batchSize = Integer.parseInt(context.get("batch_size"));
		String output = context.get("trace_batches");
		String vcfHdfsDir = HdfsUtil.path(context.getHdfsTemp(), "genotypes");
		try {

			FileSystem hdfs = FileSystem.get(HdfsUtil.getConfiguration());

			// put all vcfs in hdfs folder
			String[] files = FileUtil.getFiles(genotypes, "*.vcf.gz$|*.vcf$");
			for (String file : files) {
				context.log("Put file " + file);
				HdfsUtil.put(file, HdfsUtil.path(vcfHdfsDir, FileUtil.getFilename(file)));
			}

			// read number of samples from first vcf file
			VcfFile vcfFile = VcfFileUtil.load(files[0], 200000, false);

			int nIndividuals = vcfFile.getNoSamples();
			int batch = 0;
			int start = 1;
			int end;

			while (start <= nIndividuals) {
				end = start + batchSize - 1;
				if (end > nIndividuals) {
					end = nIndividuals;
				}

				TraceBatch traceBatch = new TraceBatch();
				traceBatch.setBatch(String.format("%05d", batch));
				traceBatch.setStart(start);
				traceBatch.setEnd(end);
				traceBatch.setStudyVcf(vcfHdfsDir);
				traceBatch.setDim(Integer.parseInt(context.get("dim")));
				traceBatch.setDimHigh(Integer.parseInt(context.get("dim_high")));
				traceBatch.setOutputStudyPc(context.get("study_pc"));
				traceBatch.setOutputStudyPopulation(context.get("study_population"));

				Gson gson = new Gson();
				BufferedWriter writer = new BufferedWriter(
						new OutputStreamWriter(hdfs.create(new Path(output, batch + ".batch"))));
				writer.write(gson.toJson(traceBatch));
				writer.close();

				start = end + 1;
				batch++;

				context.log("\t- Created batch No. " + batch);
			}

			context.ok("Prepared " + batch + " batch job" + ((batch > 1) ? "s." : "."));
			return true;

		} catch (IOException e) {
			context.error("An internal server error occured.");
			e.printStackTrace();
		}

		context.error("Execution failed. Please, contact administrator.");

		return false;
	}

}
