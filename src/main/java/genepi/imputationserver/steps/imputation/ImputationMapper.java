package genepi.imputationserver.steps.imputation;

import genepi.hadoop.CacheStore;
import genepi.hadoop.HdfsUtil;
import genepi.hadoop.ParameterStore;
import genepi.hadoop.PreferenceStore;
import genepi.hadoop.log.Log;
import genepi.imputationserver.steps.imputation.sort.ChunkKey;
import genepi.imputationserver.steps.imputation.sort.ChunkValue;
import genepi.imputationserver.steps.vcf.VcfChunk;
import genepi.imputationserver.steps.vcf.VcfChunkOutput;
import genepi.io.FileUtil;
import genepi.io.bed.BedUtil;
import genepi.io.text.LineReader;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ImputationMapper extends
		Mapper<LongWritable, Text, ChunkKey, ChunkValue> {

	private ImputationPipeline pipeline;

	public String folder;

	private String pattern;

	private String phasing;

	private ChunkKey chunkKey = new ChunkKey();

	private ChunkValue chunkValue = new ChunkValue();

	private String output;

	private String refFilename = "";

	private String mapShapeITFilename = "";
	
	private String mapHapiURFilename = "";

	private Log log;

	private String mapShapeITPattern;
	
	private String mapHapiURPattern;

	protected void setup(Context context) throws IOException,
			InterruptedException {

		log = new Log(context);

		// get parameters
		ParameterStore parameters = new ParameterStore(context);
		pattern = parameters.get(ImputationJob.REF_PANEL_PATTERN);
		mapShapeITPattern = parameters.get(ImputationJob.MAP_SHAPEIT_PATTERN);
		mapHapiURPattern = parameters.get(ImputationJob.MAP_HAPIUR_PATTERN);
		output = parameters.get(ImputationJob.OUTPUT);
		phasing = parameters.get(ImputationJob.PHASING);
		String hdfsPath = parameters.get(ImputationJob.REF_PANEL_HDFS);
		String hdfsPathShapeITMap = parameters.get(ImputationJob.MAP_SHAPEIT_HDFS);
		String hdfsPathHapiURMap = parameters.get(ImputationJob.MAP_HAPIUR_HDFS);
		String referencePanel = FileUtil.getFilename(hdfsPath);
		String mapShapeIT = FileUtil.getFilename(hdfsPathShapeITMap);
		String mapHapiUR = FileUtil.getFilename(hdfsPathHapiURMap);
		String minimacBin = parameters.get(ImputationJob.MINIMAC_BIN);

		// get cached files
		CacheStore cache = new CacheStore(context.getConfiguration());
		refFilename = cache.getArchive(referencePanel);
		mapShapeITFilename = cache.getArchive(mapShapeIT);
		mapHapiURFilename = cache.getArchive(mapHapiUR);
		String minimacCommand = cache.getFile(minimacBin);
		String hapiUrCommand = cache.getFile("hapi-ur");
		String hapiUrPreprocessCommand = cache.getFile("insert-map.pl");
		String vcfCookerCommand = cache.getFile("vcfCooker");
		String vcf2HapCommand = cache.getFile("vcf2hap");
		String shapeItCommand = cache.getFile("shapeit");

		// create temp directory
		PreferenceStore store = new PreferenceStore(context.getConfiguration());
		folder = store.getString("minimac.tmp");
		folder = FileUtil.path(folder, context.getTaskAttemptID().toString());
		FileUtil.createDirectory(folder);

		// load config
		int minimacWindow = Integer.parseInt(store.getString("minimac.window"));
		int phasingWindow = Integer.parseInt(store.getString("phasing.window"));

		// config pipeline
		pipeline = new ImputationPipeline();
		pipeline.setMinimacCommand(minimacCommand);
		pipeline.setHapiUrCommand(hapiUrCommand);
		pipeline.setHapiUrPreprocessCommand(hapiUrPreprocessCommand);
		pipeline.setVcfCookerCommand(vcfCookerCommand);
		pipeline.setVcf2HapCommand(vcf2HapCommand);
		pipeline.setShapeItCommand(shapeItCommand);
		pipeline.setMinimacWindow(minimacWindow);
		pipeline.setPhasingWindow(phasingWindow);

	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {

		// delete temp directory
		log.close();
		FileUtil.deleteDirectory(folder);

	}

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		try {

			if (value.toString() == null || value.toString().isEmpty()) {
				return;
			}

			VcfChunk chunk = new VcfChunk(value.toString());
			VcfChunkOutput outputChunk = new VcfChunkOutput(chunk, folder);

			HdfsUtil.get(chunk.getVcfFilename(), outputChunk.getVcfFilename());

			log.info("Starting pipeline for chunk " + chunk + "...");

			String chrFilename = pattern.replaceAll("\\$chr",
					chunk.getChromosome());
			String refPanelFilename = FileUtil.path(refFilename, chrFilename);

			if (!new File(refPanelFilename).exists()) {
				log.stop(
						"ReferencePanel '" + refPanelFilename + "' not found.",
						"");
			}

			pipeline.init();
			pipeline.setReferencePanel(refPanelFilename);

			if (chunk.isPhased()) {

				// convert vcf to hap
				long time = System.currentTimeMillis();
				boolean successful = pipeline.vcfToHap(outputChunk);
				time = (System.currentTimeMillis() - time) / 1000;
				if (successful) {
					log.info("  vcf2hap successful. [" + time + " sec]");
				} else {
					log.stop("  vcf2hap failed [" + time + " sec]", "");
					return;
				}

				// imputation
				if (!chunk.getChromosome().equals("23")
						&& !chunk.getChromosome().equals("X")) {

					time = System.currentTimeMillis();
					successful = pipeline.imputeMach(chunk, outputChunk);
					time = (System.currentTimeMillis() - time) / 1000;
					if (successful) {
						log.info("  Minimac successful.[" + time + " sec]");
					} else {
						log.stop("  Minimac failed[" + time + " sec]", "");
						return;
					}
				}

			} else {

				System.out.println("vcf lines: "
						+ FileUtil.getLineCount(outputChunk.getVcfFilename()));

				// convert vcf to bim/bed/fam
				long time = System.currentTimeMillis();
				boolean successful = pipeline.vcfToBed(outputChunk);
				time = (System.currentTimeMillis() - time) / 1000;

				if (successful) {
					log.info("  vcfCooker successful [" + time + " sec]");
				} else {
					log.stop("  vcfCooker failed[" + time + " sec]", "");
					return;
				}

				// ignore small chunks
				int noSnps = pipeline.getNoSnps(chunk, outputChunk);
				if (noSnps <= 2) {
					log.info("  Chunk " + chunk + " has only " + noSnps
							+ " markers. Ignore it.");
					return;
				} else {
					log.info("  Before imputation: " + noSnps + " SNPs");
				}

				// remove parents
				BedUtil.removeParents(outputChunk.getFamFilename());

				// phasing
				if (!phasing.equals("shapeit")) {
					
					// hapiur
					time = System.currentTimeMillis();
					
					chrFilename = mapHapiURPattern
							.replaceAll("\\$chr", chunk.getChromosome());
					String mapNname = FileUtil.path(mapHapiURFilename, chrFilename);

					if (!new File(mapNname).exists()) {
						log.stop("Map '" + mapNname + "' not found.", "");
					}
					
					pipeline.setMapFilename(mapNname);

				
					successful = pipeline.phaseWithHapiUr(chunk, outputChunk);
					time = (System.currentTimeMillis() - time) / 1000;

					if (successful) {
						log.info("  HapiUR successful [" + time + " sec]");
					} else {
						log.stop("  HapiUR failed [" + time + " sec]", "");
						return;
					}

				} else {
					
					
					chrFilename = mapShapeITPattern
							.replaceAll("\\$chr", chunk.getChromosome());
					String mapfilePath = FileUtil.path(mapShapeITFilename, chrFilename);

					if (!new File(mapfilePath).exists()) {
						log.stop("Map '" + mapfilePath + "' not found.", "");
					}
					
					pipeline.setMapFilename(mapfilePath);

					// shapeit
					time = System.currentTimeMillis();
					successful = pipeline.phaseWithShapeIt(chunk, outputChunk);
					time = (System.currentTimeMillis() - time) / 1000;

					if (successful) {
						log.info("  ShapeIt successful [" + time + " sec]");
					} else {
						log.stop("  ShapeIt failed [" + time + " sec]", "");
						return;
					}

				}

				// imputation
				if (!chunk.getChromosome().equals("23")
						&& !chunk.getChromosome().equals("X")) {

					time = System.currentTimeMillis();
					successful = pipeline.imputeShapeIt(chunk, outputChunk);
					time = (System.currentTimeMillis() - time) / 1000;

					if (successful) {
						log.info("  Minimac successful. [" + time + " sec]");
					} else {

						String stdOut = FileUtil.readFileAsString(outputChunk
								.getPrefix() + ".minimac.out");
						String stdErr = FileUtil.readFileAsString(outputChunk
								.getPrefix() + ".minimac.err");

						log.stop("  Minimac failed [" + time + " sec]",
								"StdOut:\n" + stdOut + "\nStdErr:\n" + stdErr);
						return;
					}

				} else {
					log.info("  Ignore  chromosome " + chunk.getChromosome()
							+ ".");
					return;
				}

			}

			// fix window bug in minimac
			long time = System.currentTimeMillis();
			int[] indices = pipeline.fixInfoFile(chunk, outputChunk);
			time = (System.currentTimeMillis() - time) / 1000;

			log.info("  Postprocessing successful. [" + time + " sec]");

			// store info file
			HdfsUtil.put(
					outputChunk.getInfoFixedFilename(),
					HdfsUtil.path(output, chunk.getChromosome(), chunk
							+ ".info"), context.getConfiguration());

			// merge dose files using shuffle and sort phase
			LineReader reader = new LineReader(outputChunk.getDoseFilename());

			while (reader.next()) {

				String line = reader.get();
				String[] tiles = line.split("\t");
				String sample = tiles[0];

				// add chunk start position
				chunkKey.setChromosome(chunk.getChromosome());
				chunkKey.setSample(sample);
				chunkKey.setStartPosition(chunk.getStart());

				StringBuffer buffer = new StringBuffer();
				for (int i = indices[0]; i <= indices[1]; i++) {
					if (i != indices[0]) {
						buffer.append("\t");
					}
					buffer.append(tiles[i + 2]);
				}

				chunkValue.genotypes = buffer.toString();
				chunkValue.sample = sample;

				context.write(chunkKey, chunkValue);

			}
			reader.close();

		} catch (Exception e) {
			log.stop("  Imputation Mapper failed.", e);
			return;
		}

	}
}
