package genepi.imputationserver.steps.imputationMinimac3;

import genepi.hadoop.CacheStore;
import genepi.hadoop.HadoopJob;
import genepi.hadoop.HdfsUtil;
import genepi.hadoop.log.LogCollector;
import genepi.io.FileUtil;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;

public class ImputationJobMinimac3 extends HadoopJob {

	public static final String REF_PANEL = "MINIMAC_REFPANEL";

	public static final String REF_PANEL_HDFS = "MINIMAC_REFPANEL_HDFS";

	public static final String MAP_SHAPEIT_HDFS = "MINIMAC_MAP_SHAPEIT_HDFS";

	public static final String MAP_SHAPEIT_PATTERN = "MINIMAC_MAP_SHAPEIT_PATTERN";

	public static final String MAP_HAPIUR_HDFS = "MINIMAC_MAP_HAPIUR_HDFS";

	public static final String MAP_HAPIUR_PATTERN = "MINIMAC_MAP_HAPIUR_PATTERN";

	public static final String REF_PANEL_EAGLE_HDFS = "MINIMAC_REFPANEL_EAGLE_HDFS";

	public static final String MAP_EAGLE_HDFS = "MINIMAC_MAP_EAGLE_HDFS";

	public static final String MAP_MINIMAC = "MINIMAC_MAP";

	public static final String POPULATION = "MINIMAC_USES_POP";

	public static final String OUTPUT = "MINIMAC_OUTPUT";

	public static final String PHASING = "MINIMAC_PHASING";

	public static final String ROUNDS = "MINIMAC_ROUNDS";

	public static final String WINDOW = "MINIMAC_WINDOW";

	public static final String MINIMAC_BIN = "MINIMAC_BIN";

	public static final String BUILD = "MINIMAC_BUILD";

	public static final String DATA_FOLDER = "minimac-data-3";

	private String refPanelHdfs;

	private String logFilename;

	private String folder;

	private String phasing;

	private boolean noCache = false;

	private String mapMinimac;

	private String mapShapeITHDFS;

	private String mapHapiURHDFS;

	private String mapEagleHDFS;

	private String refPanelEagleHDFS;

	public ImputationJobMinimac3(String name, Log log, String queue) {
		super(name, log);
		set("mapred.task.timeout", "720000000");
		set("mapred.map.tasks.speculative.execution", false);
		set("mapred.reduce.tasks.speculative.execution", false);
		log.info("setting queue to " + queue);
		getConfiguration().set("mapred.job.queue.name", queue);
		getConfiguration().set("mapred.reduce.tasks", "22");
		
		//set values times 5 due to timeout of setup
		set("mapred.tasktracker.expiry.interval", "3000000");
		set("mapred.healthChecker.script.timeout", "3000000");
		
	}

	@Override
	public void setupJob(Job job) {

		NLineInputFormat.setNumLinesPerSplit(job, 1);

		job.setMapperClass(ImputationMapperMinimac3.class);
		job.setInputFormatClass(NLineInputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setNumReduceTasks(0);

	}

	@Override
	protected void setupDistributedCache(CacheStore cache) throws IOException {
		// installs and distributed all binaries
		distribute(FileUtil.path(folder, "bin"), DATA_FOLDER, cache);

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

		// check if phasing
		if (phasing != null) {
			// add ShapeIT Map File to cache
			if (phasing.equals("shapeit") && mapShapeITHDFS != null) {
				if (HdfsUtil.exists(mapShapeITHDFS)) {
					String name = FileUtil.getFilename(mapShapeITHDFS);
					log.info("Add ShapeIT map  " + mapShapeITHDFS + " to distributed cache...");
					cache.addArchive(name, mapShapeITHDFS);
				} else {
					throw new IOException("ShapeIT map " + mapShapeITHDFS + " not found.");
				}
			}

			// add HapiUR Map File to cache
			if (phasing.equals("hapiur") && mapHapiURHDFS != null) {
				if (HdfsUtil.exists(mapHapiURHDFS)) {
					String name = FileUtil.getFilename(mapHapiURHDFS);
					log.info("Add HapiUR map  " + mapHapiURHDFS + " to distributed cache...");
					cache.addArchive(name, mapHapiURHDFS);
				} else {
					throw new IOException("HapiUR Map " + mapHapiURHDFS + " not found.");
				}
			}

			// add Eagle Map File to cache
			if (phasing.equals("eagle") && mapEagleHDFS != null) {
				if (HdfsUtil.exists(mapEagleHDFS)) {
					log.info("Add Eagle map  " + mapEagleHDFS + " to distributed cache...");
					cache.addFile(mapEagleHDFS);
				} else {
					throw new IOException("Map " + mapEagleHDFS + " not found.");
				}
			}

			// add Eagle Refpanel File for this chromosome to cache
			if (phasing.equals("eagle") && refPanelEagleHDFS != null) {
				if (!HdfsUtil.exists(refPanelEagleHDFS)) {
					throw new IOException("Eagle Reference Panel " + refPanelEagleHDFS + " not found.");
				}
				log.info("Add Eagle reference  " + refPanelEagleHDFS + " do distributed cache...");
				cache.addFile(refPanelEagleHDFS);
				log.info("Add Eagle reference  index " + refPanelEagleHDFS + ".csi to distributed cache...");
				cache.addFile(refPanelEagleHDFS + ".csi");
			}
		} else {
			log.info("No map files added to distributed cache. Input data is phased.");
		}

	}

	public void setFolder(String folder) {
		this.folder = folder;
	}

	protected void distribute(String folder, String hdfs, CacheStore cache) throws IOException {
		if (new File(folder).exists()) {
			String[] files = FileUtil.getFiles(folder, "");
			for (String file : files) {
				if (!HdfsUtil.exists(HdfsUtil.path(hdfs, FileUtil.getFilename(file))) || noCache) {
					HdfsUtil.delete(HdfsUtil.path(hdfs, FileUtil.getFilename(file)));
					HdfsUtil.put(file, HdfsUtil.path(hdfs, FileUtil.getFilename(file)));
				}
				cache.addFile(HdfsUtil.path(hdfs, FileUtil.getFilename(file)));
			}
		} else {
			log.warn("Local binary folder not found. Distribute all from hdfs.");
			List<String> files = HdfsUtil.getFiles(hdfs);
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

	public void setPopulation(String population) {
		set(POPULATION, population);
	}

	public void setPhasing(String phasing) {
		set(PHASING, phasing);
		this.phasing = phasing;
	}

	public void setRounds(String rounds) {
		set(ROUNDS, rounds);
	}

	public void setWindow(String window) {
		set(WINDOW, window);
	}

	public void setNoCache(boolean noCache) {
		this.noCache = noCache;
	}

	public void setMinimacBin(String minimacBin) {
		set(MINIMAC_BIN, minimacBin);
	}

	public void setMapShapeITPattern(String mapPattern) {
		set(MAP_SHAPEIT_PATTERN, mapPattern);
	}

	public void setMapShapeITHdfs(String mapHDFS1) {
		this.mapShapeITHDFS = mapHDFS1;
		set(MAP_SHAPEIT_HDFS, mapHDFS1);
	}

	public void setMapHapiURPattern(String mapPattern) {
		set(MAP_HAPIUR_PATTERN, mapPattern);
	}

	public void setMapHapiURHdfs(String mapHDFS2) {
		this.mapHapiURHDFS = mapHDFS2;
		set(MAP_HAPIUR_HDFS, mapHDFS2);
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

	public void setBuild(String build) {
		set(BUILD, build);
	}

}