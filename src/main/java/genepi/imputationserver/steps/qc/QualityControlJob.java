package genepi.imputationserver.steps.qc;

import genepi.hadoop.CacheStore;
import genepi.hadoop.HadoopJob;
import genepi.hadoop.HdfsUtil;
import genepi.io.FileUtil;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;

public class QualityControlJob extends HadoopJob {

	public static final String LEGEND_PATTERN = "MINIMAC_LEGEND_PATTERN";

	public static final String LEGEND_HDFS = "MINIMAC_LEGEND_HDFS";

	public static final String LEGEND_POPULATION = "MINIMAC_LEGEND_POPULATION";

	public static final String OUTPUT_MAF = "MINIMAC_OUTPUT_MAF";

	public static final String OUTPUT_MANIFEST = "MINIMAC_MANIFEST";

	public static final String OUTPUT_REMOVED_SNPS = "MINIMAC_REMOVED_SNPS";

	private String refPanelHdfs;

	private long monomorphic;
	private long alternativeAlleles;
	private long noSnps;
	private long duplicates;
	private long filtered;
	private long foundInLegend;
	private long notFoundInLegend;
	private long toLessSamples;
	private long filterFlag;
	private long invalidAlleles;
	private long remainingSnps;
	private long removedChunksSnps;
	private long removedChunksCallRate;
	private long removedChunksOverlap;

	private long alleleMismatch;
	private long alleleSwitch;
	private long strandSwitch1;
	private long strandSwitch2;
	private long strandSwitch3;
	private long match;

	public QualityControlJob(String name, Log log) {

		super(name, log);
		getConfiguration().set("mapred.task.timeout", "360000000");
		getConfiguration().set("mapred.reduce.tasks", "22");
		getConfiguration().set("mapred.job.queue.name", "qc");

	}

	@Override
	public void setupJob(Job job) {

		NLineInputFormat.setNumLinesPerSplit(job, 1);

		job.setMapperClass(QualityControlMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setInputFormatClass(NLineInputFormat.class);
		job.setReducerClass(QualityControlReducer.class);
		job.setNumReduceTasks(22);
	}

	@Override
	protected void setupDistributedCache(CacheStore cache) throws IOException {

		// add Legend file
		if (HdfsUtil.exists(refPanelHdfs)) {
			String name = FileUtil.getFilename(refPanelHdfs);
			cache.addArchive(name, refPanelHdfs);
		} else {
			throw new IOException("RefPanel " + refPanelHdfs + " not found.");
		}

	}

	@Override
	public void after() {
	}

	@Override
	public void cleanupJob(Job job) {

		try {

			CounterGroup counters = job.getCounters().getGroup("minimac");

			monomorphic = counters.findCounter("monomorphic").getValue();
			alternativeAlleles = counters.findCounter("alternativeAlleles")
					.getValue();
			noSnps = counters.findCounter("noSnps").getValue();
			duplicates = counters.findCounter("duplicates").getValue();
			filtered = counters.findCounter("filtered").getValue();
			foundInLegend = counters.findCounter("foundInLegend").getValue();
			notFoundInLegend = counters.findCounter("notFoundInLegend")
					.getValue();
			alleleMismatch = counters.findCounter("alleleMismatch").getValue();
			toLessSamples = counters.findCounter("toLessSamples").getValue();
			filterFlag = counters.findCounter("filterFlag").getValue();
			invalidAlleles = counters.findCounter("invalidAlleles").getValue();
			remainingSnps = counters.findCounter("remainingSnps").getValue();
			removedChunksSnps = counters.findCounter("removedChunksSnps")
					.getValue();
			removedChunksOverlap = counters.findCounter("removedChunksOverlap")
					.getValue();
			removedChunksCallRate = counters.findCounter(
					"removedChunksCallRate").getValue();
			strandSwitch1 = counters.findCounter("strandSwitch1").getValue();
			strandSwitch2 = counters.findCounter("strandSwitch2").getValue();
			strandSwitch3 = counters.findCounter("strandSwitch3").getValue();
			alleleSwitch = counters.findCounter("alleleSwitch").getValue();
			match = counters.findCounter("match").getValue();

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void setLegendPattern(String refPanelPattern) {
		set(LEGEND_PATTERN, refPanelPattern);
	}

	public void setLegendHdfs(String refPanelHdfs) {
		this.refPanelHdfs = refPanelHdfs;
		set(LEGEND_HDFS, refPanelHdfs);
	}

	public void setPopulation(String population) {
		set(LEGEND_POPULATION, population);
	}

	public void setOutputManifest(String outputManifest) {
		set(OUTPUT_MANIFEST, outputManifest);
	}

	public void setOutputRemovedSnps(String removedSnps) {
		set(OUTPUT_REMOVED_SNPS, removedSnps);
	}

	public void setOutputMaf(String outputMaf) {
		set(OUTPUT_MAF, outputMaf);
	}

	public String getRefPanelHdfs() {
		return refPanelHdfs;
	}

	public long getMonomorphic() {
		return monomorphic;
	}

	public long getAlternativeAlleles() {
		return alternativeAlleles;
	}

	public long getNoSnps() {
		return noSnps;
	}

	public long getDuplicates() {
		return duplicates;
	}

	public long getFiltered() {
		return filtered;
	}

	public long getFoundInLegend() {
		return foundInLegend;
	}

	public long getNotFoundInLegend() {
		return notFoundInLegend;
	}

	public long getAlleleMismatch() {
		return alleleMismatch;
	}

	public long getToLessSamples() {
		return toLessSamples;
	}

	public long getRemovedChunksSnps() {
		return removedChunksSnps;
	}

	public long getRemovedChunksOverlap() {
		return removedChunksOverlap;
	}

	public long getRemovedChunksCallRate() {
		return removedChunksCallRate;
	}

	public long getFilterFlag() {
		return filterFlag;
	}

	public long getInvalidAlleles() {
		return invalidAlleles;
	}

	public long getRemainingSnps() {
		return remainingSnps;
	}

	public long getAlleleSwitch() {
		return alleleSwitch;
	}

	public void setAlleleSwitch(long alleleSwitch) {
		this.alleleSwitch = alleleSwitch;
	}

	public long getMatch() {
		return match;
	}

	public void setMatch(long match) {
		this.match = match;
	}

	public long getStrandSwitch1() {
		return strandSwitch1;
	}

	public void setStrandSwitch1(long strandSwitch1) {
		this.strandSwitch1 = strandSwitch1;
	}

	public long getStrandSwitch2() {
		return strandSwitch2;
	}

	public void setStrandSwitch2(long strandSwitch2) {
		this.strandSwitch2 = strandSwitch2;
	}

	public long getStrandSwitch3() {
		return strandSwitch3;
	}

	public void setStrandSwitch3(long strandSwitch3) {
		this.strandSwitch3 = strandSwitch3;
	}

}