package genepi.imputationserver.steps.qc;

import java.io.IOException;
import java.text.DecimalFormat;

import genepi.hadoop.CacheStore;
import genepi.hadoop.HadoopJob;
import genepi.hadoop.formats.NLineInputFormat;
import genepi.io.FileUtil;
import genepi.io.text.LineWriter;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

public class QualityControlJob extends HadoopJob {

	public static final String LEGEND_PATTERN = "MINIMAC_LEGEND_PATTERN";

	public static final String LEGEND_HDFS = "MINIMAC_LEGEND_HDFS";

	public static final String LEGEND_POPULATION = "MINIMAC_LEGEND_POPULATION";

	public static final String OUTPUT_MAF = "MINIMAC_OUTPUT_MAF";

	public static final String OUTPUT_MANIFEST = "MINIMAC_MANIFEST";

	private String qcstat;

	private String refPanelHdfs;

	public QualityControlJob(String name) {

		super("maf-minimac");
		getConfiguration().set("mapred.task.timeout", "360000000");
		getConfiguration().set("mapred.reduce.tasks", "22");
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
	protected void setupDistributedCache(CacheStore cache) {

		// add Legend file
		String name = FileUtil.getFilename(refPanelHdfs);
		cache.addArchive(name, refPanelHdfs);

	}

	@Override
	public void after() {

	}

	@Override
	public void cleanupJob(Job job) {
		// write counters to file
		LineWriter writer;
		try {
			writer = new LineWriter(qcstat);
			long monomorphic = job.getCounters().getGroup("minimac")
					.findCounter("monomorphic").getValue();
			long alternativeAlleles = job.getCounters().getGroup("minimac")
					.findCounter("alternativeAlleles").getValue();
			long noSnps = job.getCounters().getGroup("minimac")
					.findCounter("noSnps").getValue();
			long duplicates = job.getCounters().getGroup("minimac")
					.findCounter("duplicates").getValue();
			long filtered = job.getCounters().getGroup("minimac")
					.findCounter("filtered").getValue();
			long foundInLegend = job.getCounters().getGroup("minimac")
					.findCounter("foundInLegend").getValue();
			long notFoundInLegend = job.getCounters().getGroup("minimac")
					.findCounter("notFoundInLegend").getValue();
			long alleleMismatch = job.getCounters().getGroup("minimac")
					.findCounter("alleleMismatch").getValue();
			long toLessSamples = job.getCounters().getGroup("minimac")
					.findCounter("toLessSamples").getValue();
			long removedChunks = job.getCounters().getGroup("minimac")
					.findCounter("removedChunks").getValue();

			DecimalFormat df = new DecimalFormat("#.00");

			writer.write("Duplicated sites: " + duplicates);
			writer.write("NonSNP sites: " + noSnps);
			writer.write("Alternative allele frequency > 0.5 sites: "
					+ alternativeAlleles);
			writer.write("Monomorphic sites: " + monomorphic);
			writer.write("Reference Overlap: "
					+ df.format(foundInLegend
							/ (double) (foundInLegend + notFoundInLegend) * 100)
					+ "% ");
			writer.write("Allele mismatch: " + alleleMismatch);
			writer.write("Excluded SNPs with a call rate of < 90%: "
					+ toLessSamples);
			writer.write("Excluded sites in total: " + filtered);
			writer.write("Excluded chunks: " + removedChunks);
			writer.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
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

	public void setOutputMaf(String outputMaf) {
		set(OUTPUT_MAF, outputMaf);
	}

	public void setQcStat(String qcstat) {
		this.qcstat = qcstat;
	}

}