package genepi.imputationserver.steps.imputation;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;

import genepi.hadoop.CacheStore;
import genepi.hadoop.HadoopJob;
import genepi.hadoop.HdfsUtil;
import genepi.hadoop.log.LogCollector;

public class ImputationJob extends HadoopJob {

	public static final String REF_PANEL = "MINIMAC_REFPANEL";

	public static final String REF_PANEL_HDFS = "MINIMAC_REFPANEL_HDFS";

	public static final String REF_PANEL_EAGLE_HDFS = "MINIMAC_REFPANEL_EAGLE_HDFS";

	public static final String REF_PANEL_BEAGLE_HDFS = "MINIMAC_REFPANEL_BEAGLE_HDFS";

	public static final String MAP_EAGLE_HDFS = "MINIMAC_MAP_EAGLE_HDFS";

	public static final String MAP_BEAGLE_HDFS = "MAP_BEAGLE_HDFS";

	public static final String MAP_MINIMAC = "MINIMAC_MAP";

	public static final String OUTPUT = "MINIMAC_OUTPUT";

	public static final String OUTPUT_SCORES = "PGS_OUTPUT";

	public static final String BUILD = "MINIMAC_BUILD";

	public static final String R2_FILTER = "R2_FILTER";

	public static final String PHASING_ONLY = "PHASING_ONLY";
	
	public static final String PHASING_REQUIRED = "PHASING_REQUIRED";

	public static final String PHASING_ENGINE = "PHASING_ENGINE";

	public static final String SCORES = "SCORES";

	private String refPanelHdfs;

	private String logFilename;

	private String mapMinimac;

	private String mapEagleHDFS;

	private String refPanelEagleHDFS;

	private String refPanelBeagleHDFS;

	private String mapBeagleHDFS;

	private String binariesHDFS;

	private List<String> scores;

	public ImputationJob(String name, Log log) {
		super(name, log);
		set("mapred.task.timeout", "10368000000");
		set("mapred.map.tasks.speculative.execution", false);
		set("mapred.reduce.tasks.speculative.execution", false);

		// set values times 5 due to timeout of setup
		set("mapred.tasktracker.expiry.interval", "3000000");
		set("mapred.healthChecker.script.timeout", "3000000");

	}

	@Override
	public void setupJob(Job job) {

		NLineInputFormat.setNumLinesPerSplit(job, 1);

		job.setMapperClass(ImputationMapper.class);
		job.setInputFormatClass(NLineInputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setNumReduceTasks(0);

	}

	@Override
	protected void setupDistributedCache(CacheStore cache) throws IOException {

		// installs and distributed all binaries
		if (binariesHDFS == null) {
			log.error("HDFS Binaries folder not set.");
			throw new IOException("HDFS Binaries folder not set.");
		}
		if (!HdfsUtil.exists(binariesHDFS)) {
			log.error("HDFS Binaries " + binariesHDFS + " not found.");
			throw new IOException("HDFS Binaries " + binariesHDFS + " not found.");
		}
		log.info("Distribute binaries " + binariesHDFS + " to distributed cache...");
		distribute(binariesHDFS, cache);

		// distributed refpanels
		if (HdfsUtil.exists(refPanelHdfs)) {
			log.info("Add Minimac reference panel  " + refPanelHdfs + " to distributed cache...");
			cache.addFile(refPanelHdfs);
		} else {
			log.error("Minimac reference panel " + refPanelHdfs + " not found.");
			throw new IOException("Minimac reference panel " + refPanelHdfs + " not found.");
		}

		// add minimac map file to cache
		if (mapMinimac != null) {
			if (HdfsUtil.exists(mapMinimac)) {
				log.info("Add Minimac map file " + mapMinimac + " to distributed cache...");
				cache.addFile(mapMinimac);
			} else {
				log.error("Minimac map file " + mapMinimac + " not found.");
				throw new IOException("Minimac map file " + mapMinimac + " not found.");
			}
		}

		// add Eagle Map File to cache
		if (mapEagleHDFS != null) {
			if (HdfsUtil.exists(mapEagleHDFS)) {
				log.info("Add Eagle map  " + mapEagleHDFS + " to distributed cache...");
				cache.addFile(mapEagleHDFS);
			} else {
				throw new IOException("Map " + mapEagleHDFS + " not found.");
			}
		}

		// add Eagle Refpanel File for this chromosome to cache
		if (refPanelEagleHDFS != null) {
			if (!HdfsUtil.exists(refPanelEagleHDFS)) {
				throw new IOException("Eagle Reference Panel " + refPanelEagleHDFS + " not found.");
			}
			log.info("Add Eagle reference  " + refPanelEagleHDFS + " do distributed cache...");
			cache.addFile(refPanelEagleHDFS);
			log.info("Add Eagle reference  index " + refPanelEagleHDFS + ".csi to distributed cache...");
			cache.addFile(refPanelEagleHDFS + ".csi");
		}

		// add Eagle Map File to cache
		if (mapBeagleHDFS != null) {
			if (HdfsUtil.exists(mapBeagleHDFS)) {
				log.info("Add Beagle map  " + mapBeagleHDFS + " to distributed cache...");
				cache.addFile(mapBeagleHDFS);
			} else {
				throw new IOException("Map " + mapBeagleHDFS + " not found.");
			}
		}

		// add Beagle Files to cache
		if (refPanelBeagleHDFS != null) {
			if (HdfsUtil.exists(refPanelBeagleHDFS)) {
				log.info("Add Beagle reference " + refPanelBeagleHDFS + " to distributed cache...");
				cache.addFile(refPanelBeagleHDFS);
			} else {
				throw new IOException("Beagle file reference " + refPanelBeagleHDFS + " not found.");
			}
		}

		// add scores to cache3
		if (scores != null) {
			log.info("Add " + scores.size() + " scores to distributed cache...");
			for (String score : scores) {
				if (HdfsUtil.exists(score)) {
					cache.addFile(score);
				} else {
					log.info("PGS score file '" + score + "' not found.");
					throw new IOException("PGS score file '" + score + "' not found.");
				}
			}
			log.info("All scores added to distributed cache.");
		}

	}

	protected void distribute(String hdfs, CacheStore cache) throws IOException {
		if (HdfsUtil.exists(hdfs)) {
			List<String> files = HdfsUtil.getFiles(hdfs, "");
			for (String file : files) {
				cache.addFile(file);
			}
		}
	}

	@Override
	public void after() {

		// delete temp directory for mapred output
		HdfsUtil.delete(HdfsUtil.path(getOutput(), "temp"));

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
		// set mapred output to temp directory
		super.setOutput(HdfsUtil.path(output, "temp"));
		set(OUTPUT, output);
	}

	public void setOutputScores(String outputScores) {
		set(OUTPUT_SCORES, outputScores);
	}

	public void setRefPanel(String refPanel) {
		set(REF_PANEL, refPanel);
	}

	public void setRefPanelHdfs(String refPanelHdfs) {
		this.refPanelHdfs = refPanelHdfs;
		set(REF_PANEL_HDFS, refPanelHdfs);
	}

	public void setLogFilename(String logFilename) {
		this.logFilename = logFilename;
	}

	public void setMapEagleHdfs(String mapHdfs) {
		this.mapEagleHDFS = mapHdfs;
		set(MAP_EAGLE_HDFS, mapHdfs);
	}

	public void setMapMinimac(String mapMinimac) {
		this.mapMinimac = mapMinimac;
		set(MAP_MINIMAC, mapMinimac);
	}

	public void setRefEagleHdfs(String refPanelHdfs) {
		this.refPanelEagleHDFS = refPanelHdfs;
		set(REF_PANEL_EAGLE_HDFS, refPanelHdfs);
	}

	public void setRefBeagleHdfs(String refPanelHdfs) {
		this.refPanelBeagleHDFS = refPanelHdfs;
		set(REF_PANEL_BEAGLE_HDFS, refPanelHdfs);
	}

	public void setMapBeagleHdfs(String mapBeagleHdfs) {
		this.mapBeagleHDFS = mapBeagleHdfs;
		set(MAP_BEAGLE_HDFS, mapBeagleHdfs);
	}

	public void setBuild(String build) {
		set(BUILD, build);
	}

	public void setR2Filter(String r2Filter) {
		set(R2_FILTER, r2Filter);
	}
	
	public void setPhasingRequired(String phasingRequired) {
		set(PHASING_REQUIRED, phasingRequired);
	}

	public void setPhasingOnly(String phasingOnly) {
		set(PHASING_ONLY, phasingOnly);
	}

	public void setPhasingEngine(String phasing) {
		set(PHASING_ENGINE, phasing);
	}

	public void setScores(List<String> scores) {

		String scoresNames = scores.stream().collect(Collectors.joining(","));
		set(SCORES, scoresNames);
		this.scores = scores;
	}

	public void setBinariesHDFS(String binariesHDFS) {
		this.binariesHDFS = binariesHDFS;
	}

}