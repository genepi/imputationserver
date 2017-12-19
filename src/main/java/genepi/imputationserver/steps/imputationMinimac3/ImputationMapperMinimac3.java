package genepi.imputationserver.steps.imputationMinimac3;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import genepi.hadoop.CacheStore;
import genepi.hadoop.HdfsUtil;
import genepi.hadoop.ParameterStore;
import genepi.hadoop.PreferenceStore;
import genepi.hadoop.log.Log;
import genepi.imputationserver.steps.vcf.VcfChunk;
import genepi.imputationserver.steps.vcf.VcfChunkOutput;
import genepi.imputationserver.util.FileMerger;
import genepi.imputationserver.util.FileMerger.BgzipSplitOutputStream;
import genepi.io.FileUtil;

public class ImputationMapperMinimac3 extends Mapper<LongWritable, Text, Text, Text> {

	private ImputationPipelineMinimac3 pipeline;

	public String folder;

	private String population;

	private String phasing;

	private String rounds;

	private String window;

	private String output;

	private String refFilename = "";

	private String mapMinimacFilename;

	private String mapShapeITPattern;

	private String mapHapiURPattern;

	private String mapShapeITFilename = "";

	private String mapHapiURFilename = "";

	private String mapEagleFilename = "";

	private String refEagleFilename = null;

	private String build = "hg19";

	private double r2Filter = 0;

	private String refEagleIndexFilename;

	private boolean debugging;

	private Log log;

	protected void setup(Context context) throws IOException, InterruptedException {

		HdfsUtil.setDefaultConfiguration(context.getConfiguration());

		log = new Log(context);

		// get parameters
		ParameterStore parameters = new ParameterStore(context);
		mapShapeITPattern = parameters.get(ImputationJobMinimac3.MAP_SHAPEIT_PATTERN);
		mapHapiURPattern = parameters.get(ImputationJobMinimac3.MAP_HAPIUR_PATTERN);
		output = parameters.get(ImputationJobMinimac3.OUTPUT);
		population = parameters.get(ImputationJobMinimac3.POPULATION);
		phasing = parameters.get(ImputationJobMinimac3.PHASING);
		rounds = parameters.get(ImputationJobMinimac3.ROUNDS);
		window = parameters.get(ImputationJobMinimac3.WINDOW);
		build = parameters.get(ImputationJobMinimac3.BUILD);
		String r2FilterString = parameters.get(ImputationJobMinimac3.R2_FILTER);
		if (r2FilterString == null) {
			r2Filter = 0;
		} else {
			r2Filter = Double.parseDouble(r2FilterString);
		}
		String hdfsPath = parameters.get(ImputationJobMinimac3.REF_PANEL_HDFS);
		String hdfsPathMinimacMap = parameters.get(ImputationJobMinimac3.MAP_MINIMAC);
		String hdfsPathShapeITMap = parameters.get(ImputationJobMinimac3.MAP_SHAPEIT_HDFS);
		String hdfsPathHapiURMap = parameters.get(ImputationJobMinimac3.MAP_HAPIUR_HDFS);
		String hdfsPathMapEagle = parameters.get(ImputationJobMinimac3.MAP_EAGLE_HDFS);
		String hdfsRefEagle = parameters.get(ImputationJobMinimac3.REF_PANEL_EAGLE_HDFS);

		String minimacBin = parameters.get(ImputationJobMinimac3.MINIMAC_BIN);

		// get cached files
		CacheStore cache = new CacheStore(context.getConfiguration());
		String referencePanel = FileUtil.getFilename(hdfsPath);
		refFilename = cache.getFile(referencePanel);

		if (hdfsPathMinimacMap != null) {
			System.out.println("Minimac map file hdfs: " + hdfsPathMinimacMap);
			String mapMinimac = FileUtil.getFilename(hdfsPathMinimacMap);
			System.out.println("Name: " + mapMinimac);
			mapMinimacFilename = cache.getFile(mapMinimac);
			System.out.println("Minimac map file local: " + mapMinimacFilename);

		} else {
			System.out.println("No minimac map file set.");
		}
		if (hdfsPathShapeITMap != null) {
			String mapShapeIT = FileUtil.getFilename(hdfsPathShapeITMap);
			mapShapeITFilename = cache.getArchive(mapShapeIT);
		}
		if (hdfsPathHapiURMap != null) {
			String mapHapiUR = FileUtil.getFilename(hdfsPathHapiURMap);
			mapHapiURFilename = cache.getArchive(mapHapiUR);
		}
		if (hdfsPathMapEagle != null) {
			String mapEagle = FileUtil.getFilename(hdfsPathMapEagle);
			mapEagleFilename = cache.getFile(mapEagle);
		}
		if (hdfsRefEagle != null) {
			refEagleFilename = cache.getFile(FileUtil.getFilename(hdfsRefEagle));
			refEagleIndexFilename = cache.getFile(FileUtil.getFilename(hdfsRefEagle + ".csi"));
		}

		String minimacCommand = cache.getFile(minimacBin);
		String hapiUrCommand = cache.getFile("hapi-ur");
		String hapiUrPreprocessCommand = cache.getFile("insert-map.pl");
		String vcfCookerCommand = cache.getFile("vcfCooker");
		String shapeItCommand = cache.getFile("shapeit");
		String eagleCommand = cache.getFile("eagle");
		String tabixCommand = cache.getFile("tabix");

		// create temp directory
		PreferenceStore store = new PreferenceStore(context.getConfiguration());
		folder = store.getString("minimac.tmp");
		folder = FileUtil.path(folder, context.getTaskAttemptID().toString());
		boolean created = FileUtil.createDirectory(folder);

		if (!created) {
			throw new IOException(store.getString("minimac.tmp") + " is not writable!");
		}

		// create symbolic link --> index file is in the same folder as data
		if (refEagleFilename != null) {
			Files.createSymbolicLink(Paths.get(FileUtil.path(folder, "ref.bcf")), Paths.get(refEagleFilename));
			Files.createSymbolicLink(Paths.get(FileUtil.path(folder, "ref.bcf.csi")), Paths.get(refEagleIndexFilename));
			// update reference path to symbolic link
			refEagleFilename = FileUtil.path(folder, "ref.bcf");
		}

		// read debugging flag
		String debuggingString = store.getString("debugging");
		if (debuggingString == null || debuggingString.equals("false")) {
			debugging = false;
		} else {
			debugging = true;
		}

		int phasingWindow = Integer.parseInt(store.getString("phasing.window"));

		// config pipeline
		pipeline = new ImputationPipelineMinimac3();
		pipeline.setMinimacCommand(minimacCommand);
		pipeline.setHapiUrCommand(hapiUrCommand);
		pipeline.setVcfCookerCommand(vcfCookerCommand);
		pipeline.setShapeItCommand(shapeItCommand);
		pipeline.setEagleCommand(eagleCommand);
		pipeline.setTabixCommand(tabixCommand);
		pipeline.setHapiUrPreprocessCommand(hapiUrPreprocessCommand);
		pipeline.setPhasingWindow(phasingWindow);
		pipeline.setBuild(build);

		// Minimac3
		pipeline.setRounds(Integer.parseInt(rounds));
		pipeline.setMinimacWindow(Integer.parseInt(window));

	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		// delete temp directory
		log.close();
		FileUtil.deleteDirectory(folder);
		System.out.println("Delete temp folder.");
	}

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		try {

			if (value.toString() == null || value.toString().isEmpty()) {
				return;
			}

			VcfChunk chunk = new VcfChunk(value.toString());

			VcfChunkOutput outputChunk = new VcfChunkOutput(chunk, folder);

			HdfsUtil.get(chunk.getVcfFilename(), outputChunk.getVcfFilename());

			pipeline.setRefFilename(refFilename);
			pipeline.setMapMinimac(mapMinimacFilename);
			pipeline.setMapShapeITPattern(mapShapeITPattern);
			pipeline.setMapShapeITFilename(mapShapeITFilename);
			pipeline.setMapHapiURFilename(mapHapiURFilename);
			pipeline.setMapHapiURPattern(mapHapiURPattern);
			pipeline.setMapEagleFilename(mapEagleFilename);
			pipeline.setRefEagleFilename(refEagleFilename);
			pipeline.setPhasing(phasing);
			pipeline.setPopulation(population);

			boolean succesful = pipeline.execute(chunk, outputChunk);
			if (succesful) {
				log.info("Imputation for chunk " + chunk + " successful.");
			} else {
				log.stop("Imputation failed!", "");
				return;
			}

			// store info file
			HdfsUtil.put(outputChunk.getInfoFilename(), HdfsUtil.path(output, chunk + ".info"));

			long start = System.currentTimeMillis();

			// store vcf file (remove header)
			BgzipSplitOutputStream outData = new BgzipSplitOutputStream(
					HdfsUtil.create(HdfsUtil.path(output, chunk + ".data.dose.vcf.gz")));

			BgzipSplitOutputStream outHeader = new BgzipSplitOutputStream(
					HdfsUtil.create(HdfsUtil.path(output, chunk + ".header.dose.vcf.gz")));

			FileMerger.splitIntoHeaderAndData(outputChunk.getImputedVcfFilename(), outHeader, outData, r2Filter);
			long end = System.currentTimeMillis();

			System.out.println("Time filter and put: " + (end - start) + " ms");

		} catch (Exception e) {
			if (!debugging) {
				System.out.println("Mapper Task failed.");
				cleanup(context);
			}
			throw e;
		}
	}
}
