package genepi.imputationserver.steps.imputationMinimac3;

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

public class ImputationMapperMinimac3 extends
		Mapper<LongWritable, Text, ChunkKey, ChunkValue> {

	private ImputationPipelineMinimac3 pipeline;

	public String folder;

	private String pattern;

	private String phasing;

	private String rounds;
	
	private String window;
	
	private String output;

	private String refFilename = "";

	private Log log;

	protected void setup(Context context) throws IOException,
			InterruptedException {

		log = new Log(context);

		// get parameters
		ParameterStore parameters = new ParameterStore(context);
		pattern = parameters.get(ImputationJobMinimac3.REF_PANEL_PATTERN);
		output = parameters.get(ImputationJobMinimac3.OUTPUT);
		phasing = parameters.get(ImputationJobMinimac3.PHASING);
		rounds = parameters.get(ImputationJobMinimac3.ROUNDS);
		window = parameters.get(ImputationJobMinimac3.WINDOW);
		String hdfsPath = parameters.get(ImputationJobMinimac3.REF_PANEL_HDFS);
		String referencePanel = FileUtil.getFilename(hdfsPath);
		String minimacBin = parameters.get(ImputationJobMinimac3.MINIMAC_BIN);
		
		
		// get cached files
		CacheStore cache = new CacheStore(context.getConfiguration());
		refFilename = cache.getArchive(referencePanel);
		String minimacCommand = cache.getFile(minimacBin);
		String hapiUrCommand = cache.getFile("hapi-ur");
		String vcfCookerCommand = cache.getFile("vcfCooker");
		String vcf2HapCommand = cache.getFile("vcf2hap");
		String shapeItCommand = cache.getFile("shapeit");

		// create temp directory
		PreferenceStore store = new PreferenceStore(context.getConfiguration());
		folder = store.getString("minimac.tmp");
		folder = FileUtil.path(folder, context.getTaskAttemptID().toString());
		FileUtil.createDirectory(folder);

		// load config
		//int minimacWindow = Integer.parseInt(store.getString("minimac.window"));
		int phasingWindow = Integer.parseInt(store.getString("phasing.window"));

		// config pipeline
		pipeline = new ImputationPipelineMinimac3();
		pipeline.setMinimacCommand(minimacCommand);
		pipeline.setHapiUrCommand(hapiUrCommand);
		pipeline.setVcfCookerCommand(vcfCookerCommand);
		pipeline.setVcf2HapCommand(vcf2HapCommand);
		pipeline.setShapeItCommand(shapeItCommand);
		//pipeline.setMinimacWindow(minimacWindow);
		pipeline.setPhasingWindow(phasingWindow);
		
		//Minimac3
		pipeline.setRounds(Integer.parseInt(rounds));
		pipeline.setMinimacWindow(Integer.parseInt(window));

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

		if (value.toString() == null || value.toString().isEmpty()) {
			return;
		}

		VcfChunk chunk = new VcfChunk(value.toString());
		
		VcfChunkOutput outputChunk = new VcfChunkOutput(chunk, folder);
		
		HdfsUtil.get(chunk.getVcfFilename(), outputChunk.getVcfFilename());

		log.info("Starting pipeline for chunk " + chunk + "...");

		String chrFilename = pattern
				.replaceAll("\\$chr", chunk.getChromosome());
		
		String refPanelFilename = FileUtil.path(refFilename, chrFilename);

		if (!new File(refPanelFilename).exists()) {
			log.stop("ReferencePanel '" + refPanelFilename + "' not found.", "");
		}

		pipeline.init();
		pipeline.setReferencePanel(refPanelFilename);

		if (chunk.isPhased()) {

		// imputation for phased genotypes
			if (!chunk.getChromosome().equals("23")
					&& !chunk.getChromosome().equals("X")) {

				boolean successful = pipeline.imputeVCF(chunk, outputChunk);
				if (successful) {
					log.info("  Minimac3 successful.");
				} else {
					log.stop("  Minimac3 failed", "");
					return;
				}
			}

		} else {

			System.out.println("vcf lines: "
					+ FileUtil.getLineCount(outputChunk.getVcfFilename()));

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
			//TODO ask lukas if this is still needed
			BedUtil.removeParents(outputChunk.getFamFilename());

			// phasing
			if (!phasing.equals("shapeit")) {
				
				// convert vcf to bim/bed/fam only for HapiUR
				boolean successful = pipeline.vcfToBed(outputChunk);
				if (successful) {
					log.info("  vcfCooker successful.");
				} else {
					log.stop("  vcfCooker failed", "");
					return;
				}

				// hapiur
				successful = pipeline.phaseWithHapiUr(chunk, outputChunk);
				if (successful) {
					log.info("  HapiUR successful.");
				} else {
					log.stop("  HapiUR failed", "");
					return;
				}

			} else {

				// shapeit
				boolean successful = pipeline.phaseWithShapeIt(chunk, outputChunk);
				if (successful) {
					log.info("  ShapeIt successful.");
				} else {
					log.stop("  ShapeIt failed", "");
					return;
				}

			}

			//TODO currently not supported with minmac3. 
			//use shapeit-convert or add option to handle unphased data
			
			// imputation for unphased genotypes. 
			if (!chunk.getChromosome().equals("23")
					&& !chunk.getChromosome().equals("X")) {

				boolean successful = pipeline.imputeShapeIt(chunk, outputChunk);
				if (successful) {
					log.info("  Minimac3 successful.");
				} else {

					String stdOut = FileUtil.readFileAsString(outputChunk
							.getPrefix() + ".minimac.out");
					String stdErr = FileUtil.readFileAsString(outputChunk
							.getPrefix() + ".minimac.err");

					log.stop("  Minimac3 failed", "StdOut:\n" + stdOut
							+ "\nStdErr:\n" + stdErr);
					return;
				}
			}

		}

		// fix window bug in minimac
		//TODO ask lukas what this is for
		int[] indices = pipeline.fixInfoFile(chunk, outputChunk);
		log.info("  Postprocessing successful.");

		// store info file
		HdfsUtil.put(outputChunk.getInfoFixedFilename(),
				HdfsUtil.path(output, chunk.getChromosome(), chunk + ".info"),
				context.getConfiguration());
		
		// store vcf file
				HdfsUtil.put(outputChunk.getVcfOutFilename(),
						HdfsUtil.path(output, chunk.getChromosome(), chunk + ".dose.vcf"),
						context.getConfiguration());

	}
}
