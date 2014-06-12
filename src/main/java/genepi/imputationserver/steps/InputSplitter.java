package genepi.imputationserver.steps;

import genepi.hadoop.HdfsUtil;
import genepi.hadoop.io.HdfsLineWriter;
import genepi.imputationserver.steps.vcf.VcfFile;
import genepi.imputationserver.steps.vcf.VcfFileUtil;
import genepi.io.FileUtil;
import cloudgene.mapred.jobs.CloudgeneContext;
import cloudgene.mapred.jobs.CloudgeneStep;
import cloudgene.mapred.wdl.WdlStep;

public class InputSplitter extends CloudgeneStep {

	@Override
	public boolean run(WdlStep step, CloudgeneContext context) {

		String inputFiles = context.get("files");
		String chunkfile = context.get("chunkfile");
		int chunkSize = Integer.parseInt(context.get("chunksize"));

		String files = FileUtil.path(context.getLocalTemp(), "input");

		// exports files from hdfs
		try {

			HdfsUtil.getFolder(inputFiles, files);

		} catch (Exception e) {
			error("Downloading files: " + e.getMessage());
			return false;

		}

		int pairId = 0;

		String[] vcfFiles = FileUtil.getFiles(files, "*.vcf.gz$|*.vcf$");

		for (String filename : vcfFiles) {

			try {

				VcfFile vcfFile = VcfFileUtil.load(filename, chunkSize, null);

				if (VcfFileUtil.isAutosomal(vcfFile.getChromosome())) {

					// writes chunk-file

					String chromosome = vcfFile.getChromosome();
					String type = vcfFile.getType();

					String chunkfileChr = HdfsUtil.path(chunkfile, chromosome);
					HdfsLineWriter writer = new HdfsLineWriter(chunkfileChr);

					// puts converted files into hdfs
					int i = 0;
					String[] hdfsFiles = new String[vcfFile.getFilenames().length];
					for (String filename2 : vcfFile.getFilenames()) {
						String hdfsFile = HdfsUtil.path(context.getHdfsTemp(),
								type + "_chr" + chromosome + "_" + pairId + "_"
										+ i + "");
						HdfsUtil.put(filename2, hdfsFile);
						hdfsFiles[i] = hdfsFile;
						i++;
					}

					for (int chunk : vcfFile.getChunks()) {

						int start = chunk * chunkSize + 1;
						int end = start + chunkSize - 1;

						String value = chromosome + "\t" + start + "\t" + end
								+ "\t" + type;
						for (String hdfsFile : hdfsFiles) {
							value = value + "\t" + hdfsFile;
						}

						writer.write(value);
					}
					pairId++;

					writer.close();

				}

			} catch (Exception e) {
				e.printStackTrace();
				return false;
			}

		}

		return true;
	}

}
