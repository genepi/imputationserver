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
import genepi.imputationserver.util.ImputationParameters;
import genepi.io.FileUtil;
import genepi.io.text.LineReader;
import genepi.io.text.LineWriter;

public class ImputationMapper extends Mapper<LongWritable, Text, Text, Text> {

	private ImputationPipeline pipeline;

	public String folder;

	private String output;

	private String outputScores;

	private String scores;

	private String includeScoresFilename = null;

	private String refFilename = "";

	private String mapMinimacFilename;

	private String mapEagleFilename = "";

	private String refEagleFilename = null;

	private String refBeagleFilename = null;

	private String mapBeagleFilename = "";

	private ImputationParameters imputationParameters = null;

	private String build = "hg19";

	private boolean phasingOnly = false;

	private boolean phasingRequired = true;

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

		double minR2;
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

		String phasingRequiredString = parameters.get(ImputationJob.PHASING_REQUIRED);

		if (phasingRequiredString == null) {
			phasingRequired = true;
		} else {
			phasingRequired = Boolean.parseBoolean(phasingRequiredString);
		}

		phasingEngine = parameters.get(ImputationJob.PHASING_ENGINE);

		hdfsPath = parameters.get(ImputationJob.REF_PANEL_HDFS);
		String hdfsPathMinimacMap = parameters.get(ImputationJob.MAP_MINIMAC);
		String hdfsPathMapEagle = parameters.get(ImputationJob.MAP_EAGLE_HDFS);
		String hdfsRefEagle = parameters.get(ImputationJob.REF_PANEL_EAGLE_HDFS);
		String hdfsRefBeagle = parameters.get(ImputationJob.REF_PANEL_BEAGLE_HDFS);
		String hdfsPathMapBeagle = parameters.get(ImputationJob.MAP_BEAGLE_HDFS);

		// set object
		imputationParameters = new ImputationParameters();
		String referenceName = parameters.get(ImputationJob.REF_PANEL);
		imputationParameters.setPhasing(phasingEngine);
		imputationParameters.setReferencePanelName(referenceName);
		imputationParameters.setPhasingRequired(phasingRequired);

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

		String minimacCommand = cache.getFile("minimac4");
		String eagleCommand = cache.getFile("eagle");
		String beagleCommand = cache.getFile("beagle.jar");
		String tabixCommand = cache.getFile("tabix");

		// create temp directory
		DefaultPreferenceStore store = new DefaultPreferenceStore(context.getConfiguration());
		folder = store.getString("minimac.tmp");
		folder = FileUtil.path(folder, context.getTaskAttemptID().toString());
		boolean created = FileUtil.createDirectory(folder);

		if (!created) {
			throw new IOException(folder + " is not writable!");
		}

		// scores
		String scoresFilename = parameters.get(ImputationJob.SCORE_FILE);
		if (scoresFilename != null) {
			String name = FileUtil.getFilename(scoresFilename);
			String localFilename = cache.getFile(name);
			scores = localFilename;
			// check if score file has info and tbi file
			String infoFile = cache.getFile(name + ".info");
			String tbiFile = cache.getFile(name + ".tbi");
			if (infoFile != null && tbiFile != null) {
				// create symbolic link to format file. they have to be in the same folder
				Files.createSymbolicLink(Paths.get(FileUtil.path(folder, name)), Paths.get(localFilename));
				Files.createSymbolicLink(Paths.get(FileUtil.path(folder, name + ".info")), Paths.get(infoFile));
				Files.createSymbolicLink(Paths.get(FileUtil.path(folder, name + ".tbi")), Paths.get(tbiFile));
				scores = FileUtil.path(folder, name);
			} else {
				throw new IOException("*info or *tbi file not available");
			}
			System.out.println("Loaded " + FileUtil.getFilename(scoresFilename) + " from distributed cache");

			String hdfsIncludeScoresFilename = parameters.get(ImputationJob.INCLUDE_SCORE_FILE);
			if (hdfsIncludeScoresFilename != null){
				String includeScoresName = FileUtil.getFilename(hdfsIncludeScoresFilename);
				includeScoresFilename = cache.getFile(includeScoresName);
			}



		} else {
			System.out.println("No scores file set.");
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
		int decay = Integer.parseInt(store.getString("minimac.decay"));

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
		pipeline.setMinR2(minR2);
		pipeline.setDecay(decay);

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
			pipeline.setIncludeScoreFilename(includeScoresFilename);

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

				FileMerger.splitPhasedIntoHeaderAndData(outputChunk.getPhasedVcfFilename(), outHeader, outData, chunk,
						imputationParameters);
				long end = System.currentTimeMillis();

				statistics.setImportTime((end - start) / 1000);

			}

			// push results only if not in PGS mode
			else if (scores == null) {

				HdfsUtil.put(outputChunk.getInfoFilename(), HdfsUtil.path(output, chunk + ".info"));

				long start = System.currentTimeMillis();

				// store vcf file (remove header)
				BgzipSplitOutputStream outData = new BgzipSplitOutputStream(
						HdfsUtil.create(HdfsUtil.path(output, chunk + ".data.dose.vcf.gz")));

				BgzipSplitOutputStream outHeader = new BgzipSplitOutputStream(
						HdfsUtil.create(HdfsUtil.path(output, chunk + ".header.dose.vcf.gz")));

				FileMerger.splitIntoHeaderAndData(outputChunk.getImputedVcfFilename(), outHeader, outData,
						imputationParameters);

				// store vcf file (remove header)
				BgzipSplitOutputStream outDataMeta = new BgzipSplitOutputStream(
						HdfsUtil.create(HdfsUtil.path(output, chunk + ".data.empiricalDose.vcf.gz")));

				BgzipSplitOutputStream outHeaderMeta = new BgzipSplitOutputStream(
						HdfsUtil.create(HdfsUtil.path(output, chunk + ".header.empiricalDose.vcf.gz")));

				FileMerger.splitIntoHeaderAndData(outputChunk.getMetaVcfFilename(), outHeaderMeta, outDataMeta,
						imputationParameters);

				long end = System.currentTimeMillis();

				statistics.setImportTime((end - start) / 1000);

				System.out.println("Time filter and put: " + (end - start) + " ms");

			} else {

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

}
