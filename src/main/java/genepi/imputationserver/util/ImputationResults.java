package genepi.imputationserver.util;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import genepi.hadoop.HdfsUtil;
import genepi.io.FileUtil;

public class ImputationResults {

	private Map<String, ImputedChromosome> chromosomes = new HashMap<String, ImputedChromosome>();

	public ImputationResults(List<String> folders, boolean phasingOnly) throws IOException {

		for (String folder : folders) {

			String chromosomeName = FileUtil.getFilename(folder);

			// combine all X. to one folder
			if (chromosomeName.startsWith("X.")) {
				chromosomeName = "X";
			}

			ImputedChromosome chromsome = chromosomes.get(chromosomeName);

			if (chromsome == null) {
				chromsome = new ImputedChromosome();
				chromosomes.put(chromosomeName, chromsome);
			}

			if (phasingOnly) {

				List<String> headerFiles = findFiles(folder, ".header.dose.vcf.gz");
				chromsome.addHeaderFiles(headerFiles);

				List<String> dataFiles = findFiles(folder, ".phased.vcf.gz");
				chromsome.addDataFiles(dataFiles);

			} else {

				List<String> headerFiles = findFiles(folder, ".header.dose.vcf.gz");
				chromsome.addHeaderFiles(headerFiles);

				List<String> dataFiles = findFiles(folder, ".data.dose.vcf.gz");
				chromsome.addDataFiles(dataFiles);

				List<String> infoFiles = findFiles(folder, ".info");
				chromsome.addInfoFiles(infoFiles);

				List<String> headerMetaFiles = findFiles(folder, ".header.empiricalDose.vcf.gz");
				chromsome.addHeaderMetaFiles(headerMetaFiles);

				List<String> dataMetaFiles = findFiles(folder, ".data.empiricalDose.vcf.gz");
				chromsome.addDataMetaFiles(dataMetaFiles);

			}

			// resort for chrX only
			if (chromosomeName.equals("X")) {
				Collections.sort(chromsome.getDataMetaFiles(), new ChromosomeXComparator());
				Collections.sort(chromsome.getDataFiles(), new ChromosomeXComparator());
				Collections.sort(chromsome.getInfoFiles(), new ChromosomeXComparator());
			}
		}

	}

	public Map<String, ImputedChromosome> getChromosomes() {
		return chromosomes;
	}

	protected List<String> findFiles(String folder, String pattern) throws IOException {

		Configuration conf = HdfsUtil.getConfiguration();

		FileSystem fileSystem = FileSystem.get(conf);
		Path pathFolder = new Path(folder);
		FileStatus[] files = fileSystem.listStatus(pathFolder);

		List<String> dataFiles = new Vector<String>();
		for (FileStatus file : files) {
			if (!file.isDir() && !file.getPath().getName().startsWith("_")
					&& file.getPath().getName().endsWith(pattern)) {
				dataFiles.add(file.getPath().toString());
			}
		}
		Collections.sort(dataFiles);
		return dataFiles;
	}

	class ChromosomeXComparator implements Comparator<String> {

		List<String> definedOrder = Arrays.asList("X.PAR1", "X.nonPAR", "X.PAR2");

		@Override
		public int compare(String o1, String o2) {

			String region = o1.substring(o1.lastIndexOf("/") + 1).split("_")[1];

			String region2 = o2.substring(o2.lastIndexOf("/") + 1).split("_")[1];

			return Integer.valueOf(definedOrder.indexOf(region))
					.compareTo(Integer.valueOf(definedOrder.indexOf(region2)));
		}

	}

}
