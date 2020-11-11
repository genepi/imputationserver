package genepi.imputationserver.steps.imputation;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import genepi.hadoop.CacheStore;
import genepi.hadoop.HdfsUtil;
import genepi.hadoop.ParameterStore;
import genepi.hadoop.log.Log;
import genepi.imputationserver.steps.vcf.VcfChunk;
import genepi.imputationserver.steps.vcf.VcfChunkOutput;
import genepi.imputationserver.util.DefaultPreferenceStore;
import genepi.imputationserver.util.FileMerger;
import genepi.imputationserver.util.FileMerger.BgzipSplitOutputStream;
import genepi.io.FileUtil;
import genepi.io.text.LineReader;
import genepi.io.text.LineWriter;

public class ImputationMapper extends Mapper<LongWritable, Text, Text, Text> {

	private ImputationPipeline pipeline;

	public String folder;

	private String output;

	private String outputScores;

	private String[] scores;

	private String refFilename = "";

	private String mapMinimacFilename;

	private String mapEagleFilename = "";

	private String refEagleFilename = null;

	private String refBeagleFilename = null;

	private String mapBeagleFilename = "";

	private String build = "hg19";

	private double minR2 = 0;

	private boolean phasingOnly = false;

	private String phasingEngine = "";

	private String refEagleIndexFilename;

	private boolean debugging;

	private Log log;

	private String hdfsPath;

	protected void setup(Context context) throws IOException, InterruptedException {

		HdfsUtil.setDefaultConfiguration(context.getConfiguration());

		log = new Log(context);

		// get parameters
		ParameterStore parameters = new ParameterStore(context);

		output = parameters.get(ImputationJob.OUTPUT);
		outputScores = parameters.get(ImputationJob.OUTPUT_SCORES);
		build = parameters.get(ImputationJob.BUILD);

		String r2FilterString = parameters.get(ImputationJob.R2_FILTER);
		if (r2FilterString == null) {
			minR2 = 0;
		} else {
			minR2 = Double.parseDouble(r2FilterString);
		}

		String phasingOnlyString = parameters.get(ImputationJob.PHASING_ONLY);

		if (phasingOnlyString == null) {
			phasingOnly = false;
		} else {
			phasingOnly = Boolean.parseBoolean(phasingOnlyString);
		}

		phasingEngine = parameters.get(ImputationJob.PHASING_ENGINE);

		hdfsPath = parameters.get(ImputationJob.REF_PANEL_HDFS);
		String hdfsPathMinimacMap = parameters.get(ImputationJob.MAP_MINIMAC);
		String hdfsPathMapEagle = parameters.get(ImputationJob.MAP_EAGLE_HDFS);
		String hdfsRefEagle = parameters.get(ImputationJob.REF_PANEL_EAGLE_HDFS);
		String hdfsRefBeagle = parameters.get(ImputationJob.REF_PANEL_BEAGLE_HDFS);
		String hdfsPathMapBeagle = parameters.get(ImputationJob.MAP_BEAGLE_HDFS);

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

		if (hdfsPathMapEagle != null) {
			String mapEagle = FileUtil.getFilename(hdfsPathMapEagle);
			mapEagleFilename = cache.getFile(mapEagle);
		}
		if (hdfsRefEagle != null) {
			refEagleFilename = cache.getFile(FileUtil.getFilename(hdfsRefEagle));
			refEagleIndexFilename = cache.getFile(FileUtil.getFilename(hdfsRefEagle + ".csi"));
		}
		if (hdfsRefBeagle != null) {
			refBeagleFilename = cache.getFile(FileUtil.getFilename(hdfsRefBeagle));
		}

		if (hdfsPathMapBeagle != null) {
			String mapBeagle = FileUtil.getFilename(hdfsPathMapBeagle);
			mapBeagleFilename = cache.getFile(mapBeagle);
		}

		String minimacCommand = cache.getFile("Minimac4");
		String eagleCommand = cache.getFile("eagle");
		String beagleCommand = cache.getFile("beagle.jar");
		String tabixCommand = cache.getFile("tabix");

		// scores
		String scoresFilenames = parameters.get(ImputationJob.SCORES);
		if (scoresFilenames != null) {
			String[] filenames = scoresFilenames.split(",");
			scores = new String[filenames.length];
			for (int i = 0; i < scores.length; i++) {
				String filename = filenames[i];
				String name = FileUtil.getFilename(filename);
				String localFilename = cache.getFile(name);
				scores[i] = localFilename;
			}
			System.out.println("Loaded " + scores.length + " score files from distributed cache");

		} else {
			System.out.println("No scores files et.");
		}

		// create temp directory
		DefaultPreferenceStore store = new DefaultPreferenceStore(context.getConfiguration());
		folder = store.getString("minimac.tmp");
		folder = FileUtil.path(folder, context.getTaskAttemptID().toString());
		boolean created = FileUtil.createDirectory(folder);

		if (!created) {
			throw new IOException(folder + " is not writable!");
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

		int window = Integer.parseInt(store.getString("minimac.window"));

		String minimacParams = store.getString("minimac.command");
		String eagleParams = store.getString("eagle.command");
		String beagleParams = store.getString("beagle.command");

		// config pipeline
		pipeline = new ImputationPipeline();
		pipeline.setMinimacCommand(minimacCommand, minimacParams);
		pipeline.setEagleCommand(eagleCommand, eagleParams);
		pipeline.setBeagleCommand(beagleCommand, beagleParams);
		pipeline.setTabixCommand(tabixCommand);
		pipeline.setPhasingWindow(phasingWindow);
		pipeline.setBuild(build);
		pipeline.setMinimacWindow(window);

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

			long startTotal = System.currentTimeMillis();

			VcfChunk chunk = new VcfChunk(value.toString());

			VcfChunkOutput outputChunk = new VcfChunkOutput(chunk, folder);

			HdfsUtil.get(chunk.getVcfFilename(), outputChunk.getVcfFilename());

			pipeline.setRefFilename(refFilename);
			pipeline.setMapMinimac(mapMinimacFilename);
			pipeline.setMapEagleFilename(mapEagleFilename);
			pipeline.setRefEagleFilename(refEagleFilename);
			pipeline.setRefBeagleFilename(refBeagleFilename);
			pipeline.setMapBeagleFilename(mapBeagleFilename);
			pipeline.setPhasingEngine(phasingEngine);
			pipeline.setPhasingOnly(phasingOnly);
			pipeline.setScores(scores);

			boolean succesful = pipeline.execute(chunk, outputChunk);
			ImputationStatistic statistics = pipeline.getStatistic();

			if (!succesful) {
				log.stop("Phasing/Imputation failed!", "");
				return;
			}

			if (phasingOnly) {

				long start = System.currentTimeMillis();

				// store vcf file (remove header)
				BgzipSplitOutputStream outData = new BgzipSplitOutputStream(
						HdfsUtil.create(HdfsUtil.path(output, chunk + ".phased.vcf.gz")));

				BgzipSplitOutputStream outHeader = new BgzipSplitOutputStream(
						HdfsUtil.create(HdfsUtil.path(output, chunk + ".header.dose.vcf.gz")));

				FileMerger.splitPhasedIntoHeaderAndData(outputChunk.getPhasedVcfFilename(), outHeader, outData, chunk);
				long end = System.currentTimeMillis();

				statistics.setImportTime((end - start) / 1000);

			} else {
				if (minR2 > 0) {
					// filter by r2
					String filteredInfoFilename = outputChunk.getInfoFilename() + "_filtered";
					filterInfoFileByR2(outputChunk.getInfoFilename(), filteredInfoFilename, minR2);
					HdfsUtil.put(filteredInfoFilename, HdfsUtil.path(output, chunk + ".info"));

				} else {
					HdfsUtil.put(outputChunk.getInfoFilename(), HdfsUtil.path(output, chunk + ".info"));
				}

				long start = System.currentTimeMillis();

				// store vcf file (remove header)
				BgzipSplitOutputStream outData = new BgzipSplitOutputStream(
						HdfsUtil.create(HdfsUtil.path(output, chunk + ".data.dose.vcf.gz")));

				BgzipSplitOutputStream outHeader = new BgzipSplitOutputStream(
						HdfsUtil.create(HdfsUtil.path(output, chunk + ".header.dose.vcf.gz")));

				FileMerger.splitIntoHeaderAndData(outputChunk.getImputedVcfFilename(), outHeader, outData, minR2);
				long end = System.currentTimeMillis();

				statistics.setImportTime((end - start) / 1000);

				System.out.println("Time filter and put: " + (end - start) + " ms");

			}

			if (scores != null) {

				HdfsUtil.put(outputChunk.getScoreFilename(), HdfsUtil.path(outputScores, chunk + ".scores.txt"));
				HdfsUtil.put(outputChunk.getScoreFilename() + ".json",
						HdfsUtil.path(outputScores, chunk + ".scores.json"));

			}

			InetAddress addr = java.net.InetAddress.getLocalHost();
			String hostname = addr.getHostName();

			long endTotal = System.currentTimeMillis();

			long timeTotal = (endTotal - startTotal) / 1000;

			log.info(context.getJobName() + "\t" + hdfsPath + "\t" + hostname + "\t" + chunk + "\t"
					+ statistics.toString() + "\t" + timeTotal);

		} catch (Exception e) {
			if (!debugging) {
				System.out.println("Mapper Task failed.");
				e.printStackTrace(System.out);
				e.printStackTrace();
				cleanup(context);
			}
			throw e;
		}
	}

	public void filterInfoFileByR2(String input, String output, double minR2) throws IOException {

		LineReader readerInfo = new LineReader(input);
		LineWriter writerInfo = new LineWriter(output);

		readerInfo.next();
		String header = readerInfo.get();

		// find index for Rsq
		String[] headerTiles = header.split("\t");
		int index = -1;
		for (int i = 0; i < headerTiles.length; i++) {
			if (headerTiles[i].equals("Rsq")) {
				index = i;
			}
		}

		writerInfo.write(header);

		while (readerInfo.next()) {
			String line = readerInfo.get();
			String[] tiles = line.split("\t");
			String value = tiles[index];
			try {
				double r2 = Double.parseDouble(value);
				if (r2 > minR2) {
					writerInfo.write(line);
				}
			} catch (NumberFormatException e) {
				writerInfo.write(line);
			}
		}

		readerInfo.close();
		writerInfo.close();

	}
}
