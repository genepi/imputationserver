package genepi.imputationserver.steps.imputation;

import java.io.IOException;
import java.util.List;

import genepi.hadoop.CacheStore;
import genepi.hadoop.HadoopJob;
import genepi.hadoop.HdfsUtil;
import genepi.hadoop.formats.NLineInputFormat;
import genepi.hadoop.log.LogCollector;
import genepi.imputationserver.steps.imputation.sort.ChunkKey;
import genepi.imputationserver.steps.imputation.sort.ChunkValue;
import genepi.imputationserver.steps.imputation.sort.CompositeKeyComparator;
import genepi.imputationserver.steps.imputation.sort.NaturalKeyGroupingComparator;
import genepi.imputationserver.steps.imputation.sort.NaturalKeyPartitioner;
import genepi.io.FileUtil;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

public class ImputationJob extends HadoopJob {

	public static final String REF_PANEL = "MINIMAC_REFPANEL";

	public static final String REF_PANEL_PATTERN = "MINIMAC_REFPANEL_PATTERN";

	public static final String REF_PANEL_HDFS = "MINIMAC_REFPANEL_HDFS";

	public static final String OUTPUT = "MINIMAC_OUTPUT";

	public static final String CHUNK_SIZE = "MINIMAC_CHUNKSIZE";

	public static final String WINDOW = "MINIMAC_WINDOW";

	public static final String PHASING = "MINIMAC_PHASING";

	private String localOutput;

	private String refPanelHdfs;

	private String logFilename;

	public ImputationJob(String name) {

		super("imputation-minimac");
		set("mapred.task.timeout", "360000000");
		set("mapred.map.tasks.speculative.execution", false);
		set("mapred.reduce.tasks.speculative.execution", false);
		getConfiguration().set("mapred.reduce.tasks", "22");
	}

	@Override
	public void setupJob(Job job) {

		NLineInputFormat.setNumLinesPerSplit(job, 1);

		job.setMapperClass(ImputationMapper.class);
		job.setInputFormatClass(NLineInputFormat.class);

		job.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);
		job.setPartitionerClass(NaturalKeyPartitioner.class);
		job.setSortComparatorClass(CompositeKeyComparator.class);
		job.setMapOutputKeyClass(ChunkKey.class);
		job.setMapOutputValueClass(ChunkValue.class);

		job.setOutputKeyClass(Text.class);
		job.setNumReduceTasks(22);

		job.setReducerClass(ImputationReducer.class);

	}

	@Override
	protected void setupDistributedCache(CacheStore cache) {

		// installs and distributed alls binaries
		String data = "minimac-data";
		distribute("bin", data, cache);

		// distributed refpanels

		String name = FileUtil.getFilename(refPanelHdfs);

		cache.addArchive(name, refPanelHdfs);
	}

	protected void distribute(String folder, String hdfs, CacheStore cache) {
		String[] files = FileUtil.getFiles(folder, "");
		for (String file : files) {
			if (!HdfsUtil
					.exists(HdfsUtil.path(hdfs, FileUtil.getFilename(file)))) {
				HdfsUtil.put(file,
						HdfsUtil.path(hdfs, FileUtil.getFilename(file)));
			}
			cache.addFile(HdfsUtil.path(hdfs, FileUtil.getFilename(file)));
		}
	}

	@Override
	public void after() {

		try {

			List<String> folders = HdfsUtil.getDirectories(getOutput());

			FileUtil.createDirectory(FileUtil.path(localOutput, "results"));

			for (String folder : folders) {

				String name = FileUtil.getFilename(folder);

				// merge all info files
				HdfsUtil.mergeAndGz(
						FileUtil.path(localOutput, "results", "chr" + name
								+ ".info.gz"), folder, true, ".info");

				// export merged dose file
				HdfsUtil.get(
						HdfsUtil.path(folder, "merged.dose.gz"),
						FileUtil.path(localOutput, "results", "chr" + name
								+ ".dose.gz"));

			}

			// delete temporary files
			HdfsUtil.delete(getOutput());

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Override
	public void cleanupJob(Job job) {
		LogCollector collector = new LogCollector(job);
		try {
			collector.save(logFilename);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void setOutput(String output) {
		super.setOutput(output);
		set(OUTPUT, output);
	}

	public void setLocalOutput(String localOutput) {
		this.localOutput = localOutput;
	}

	public void setRefPanel(String refPanel) {
		set(REF_PANEL, refPanel);
	}

	public void setRefPanelPattern(String refPanelPattern) {
		set(REF_PANEL_PATTERN, refPanelPattern);
	}

	public void setRefPanelHdfs(String refPanelHdfs) {
		this.refPanelHdfs = refPanelHdfs;
		set(REF_PANEL_HDFS, refPanelHdfs);
	}

	public void setChunkSize(int size) {
		set(CHUNK_SIZE, size);
	}

	public void setWindowSize(int window) {
		set(WINDOW, window);
	}

	public void setLogFilename(String logFilename) {
		this.logFilename = logFilename;
	}

	public void setPhasing(String phasing) {
		set(PHASING, phasing);
	}

}