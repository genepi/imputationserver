package genepi.minicloudmac.hadoop.validation;

import genepi.hadoop.HdfsUtil;
import genepi.io.FileUtil;
import genepi.minicloudmac.hadoop.validation.io.vcf.VcfPair;
import genepi.minicloudmac.hadoop.validation.io.vcf.VcfPreprocessor;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import cloudgene.mapred.jobs.CloudgeneContext;
import cloudgene.mapred.jobs.CloudgeneStep;
import cloudgene.mapred.wdl.WdlStep;

public class VcfInputFileSplitter extends CloudgeneStep {

	public static Set<String> validChromosomes = new HashSet<String>();

	static {

		validChromosomes.add("1");
		validChromosomes.add("2");
		validChromosomes.add("3");
		validChromosomes.add("4");
		validChromosomes.add("5");
		validChromosomes.add("6");
		validChromosomes.add("7");
		validChromosomes.add("8");
		validChromosomes.add("9");
		validChromosomes.add("10");
		validChromosomes.add("11");
		validChromosomes.add("12");
		validChromosomes.add("13");
		validChromosomes.add("14");
		validChromosomes.add("15");
		validChromosomes.add("16");
		validChromosomes.add("17");
		validChromosomes.add("18");
		validChromosomes.add("19");
		validChromosomes.add("20");
		validChromosomes.add("21");
		validChromosomes.add("22");
	}

	@Override
	public boolean run(WdlStep step, CloudgeneContext context) {

		String folder = getFolder(VcfInputFileSplitter.class);

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

		VcfPreprocessor preprocessor = new VcfPreprocessor(chunkSize, null);
		String[] vcfFiles = FileUtil.getFiles(files, "*.vcf.gz$|*.vcf$");

		for (String filename : vcfFiles) {
			VcfPair pair = preprocessor.validate(filename);
			if (pair == null) {
				return false;
			} else {

				if (validChromosomes.contains(pair.getChromosome())) {

					// writes chunk-file
					try {
						Configuration configuration = new Configuration();
						FileSystem filesystem = FileSystem.get(configuration);

						FSDataOutputStream out = filesystem
								.create(new Path(HdfsUtil.path(chunkfile,
										pair.getChromosome())));

						// Set<String> chromosomes = new HashSet<String>();

						String chromosome = pair.getChromosome();
						String type = pair.getType();

						// puts converted files into hdfs
						int i = 0;
						String[] hdfsFiles = new String[pair.getFilenames().length];
						for (String filename2 : pair.getFilenames()) {
							String hdfsFile = HdfsUtil.path(
									context.getHdfsTemp(), type + "_chr"
											+ chromosome + "_" + pairId + "_"
											+ i + "");
							HdfsUtil.put(filename2, hdfsFile);
							hdfsFiles[i] = hdfsFile;
							i++;
						}

						for (int chunk : pair.getChunks()) {

							int start = chunk * chunkSize + 1;
							int end = start + chunkSize - 1;

							String value = pair.getChromosome() + "\t" + start
									+ "\t" + end + "\t" + type;
							for (String hdfsFile : hdfsFiles) {
								value = value + "\t" + hdfsFile;
							}

							out.writeBytes(value + "\n");
						}
						pairId++;

						out.close();

					} catch (Exception e) {
						e.printStackTrace();
						return false;
					}

				}
			}

		}

		return true;
	}

}
