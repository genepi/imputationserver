package genepi.imputationserver.steps.imputation;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.codehaus.groovy.control.CompilationFailedException;

import genepi.hadoop.command.Command;
import genepi.imputationserver.steps.vcf.VcfChunk;
import genepi.imputationserver.steps.vcf.VcfChunkOutput;
import genepi.io.FileUtil;
import groovy.text.SimpleTemplateEngine;

public class ImputationPipeline {

	public static final String PIPELINE_VERSION = "michigan-imputationserver-1.3.3";
	
	public static final String IMPUTATION_VERSION = "minimac4-1.0.2";

	public static final String PHASING_VERSION = "eagle-2.4";
	
	private String minimacCommand;
	
	private String minimacParams;
	
	private String eagleCommand;
	
	private String eagleParams;
	
	private String tabixCommand;
	
	private int minimacWindow;
	
	private int phasingWindow;

	private String refFilename;

	private String mapMinimac;

	private String mapEagleFilename = "";
	
	private String refEagleFilename = "";

	private String build = "hg19";
	
	private boolean phasingOnly;

	private ImputationStatistic statistic = new ImputationStatistic();

	private SimpleTemplateEngine engine = new SimpleTemplateEngine();

	public boolean execute(VcfChunk chunk, VcfChunkOutput output) throws InterruptedException, IOException {

		System.out.println("Starting pipeline for chunk " + chunk + " [Phased: " + chunk.isPhased() + "]...");

		if (!new File(refFilename).exists()) {
			System.out.println("ReferencePanel '" + refFilename + "' not found.");
			return false;
		}

		if (!new File(output.getVcfFilename()).exists()) {
			System.out.println("vcf.gz file not found: " + output.getVcfFilename());
			return false;
		}

		// replace X.nonpar / X.par with X needed by eagle and minimac
		if (chunk.getChromosome().startsWith("X.")) {
			output.setChromosome("X");
		}

		// create tabix index
		Command tabix = new Command(tabixCommand);
		tabix.setSilent(false);
		tabix.setParams(output.getVcfFilename());
		System.out.println("Command: " + tabix.getExecutedCommand());
		if (tabix.execute() != 0) {
			System.out.println("Error during index creation: " + tabix.getStdOut());
			return false;
		}

		if (chunk.isPhased()) {
			
			FileUtils.moveFile(new File(output.getVcfFilename()), new File(output.getPhasedVcfFilename()));
			System.out.println("Chunk already phased. Move file "+ output.getVcfFilename() + " to " + output.getPhasedVcfFilename()+ ".");

		} else {

			// eagle
			long time = System.currentTimeMillis();

			if (!new File(refEagleFilename).exists()) {
				System.out.println("Eagle: Reference '" + refEagleFilename + "' not found.");
				return false;
			}

			boolean successful = phaseWithEagle(chunk, output, refEagleFilename, mapEagleFilename);
			time = (System.currentTimeMillis() - time) / 1000;

			statistic.setPhasingTime(time);

			if (successful) {
				System.out.println("  " + PHASING_VERSION + " finished successfully. [" + time + " sec]");
			} else {
				System.out.println("  " + PHASING_VERSION + " failed. [" + time + " sec]");
				return false;
			}

		}
		
		if (phasingOnly) {
			System.out.println("Phasing-only mode, no imputation started.");
			return true;
		}


		// Imputation

		long time = System.currentTimeMillis();
		boolean successful = imputeVCF(output);
		time = (System.currentTimeMillis() - time) / 1000;

		statistic.setImputationTime(time);

		if (successful) {
			System.out.println("  " + IMPUTATION_VERSION + " finished successfully. [" + time + " sec]");
			return true;
		} else {
			String stdOut = FileUtil.readFileAsString(output.getPrefix() + ".minimac.out");
			String stdErr = FileUtil.readFileAsString(output.getPrefix() + ".minimac.err");
			System.out.println("  " + IMPUTATION_VERSION + " failed. [" + time + " sec]\n\nStdOut:\n" + stdOut
					+ "\nStdErr:\n" + stdErr);
			return false;
		}
	}

	public boolean phaseWithEagle(VcfChunk input, VcfChunkOutput output, String reference, String mapFilename)
			throws IOException {

		// calculate phasing positions
		int start = input.getStart() - phasingWindow;
		if (start < 1) {
			start = 1;
		}
		int end = input.getEnd() + phasingWindow;

		String phasedPrefix = ".eagle.phased";

		// set parameters
		Map<String, Object> binding = new HashMap<String, Object>();
		binding.put("ref", reference);
		binding.put("vcf", output.getVcfFilename());
		binding.put("map", mapFilename);
		binding.put("prefix", output.getPrefix() + phasedPrefix);
		binding.put("start", start);
		binding.put("end", end);

		String[] params = createParams(eagleParams, binding);

		// eagle command
		Command eagle = new Command(eagleCommand);
		eagle.setSilent(false);
		eagle.setParams(params);
		eagle.saveStdOut(output.getPrefix() + ".eagle.out");
		eagle.saveStdErr(output.getPrefix() + ".eagle.err");
		System.out.println("Command: " + eagle.getExecutedCommand());
		if (eagle.execute() != 0) {
			return false;
		}

		// rename
		new File(output.getPrefix() + phasedPrefix + ".vcf.gz").renameTo(new File(output.getPhasedVcfFilename()));

		// haps to vcf
		return true;
	}

	public boolean imputeVCF(VcfChunkOutput output)
			throws InterruptedException, IOException, CompilationFailedException {

		String chr = "";
		if (build.equals("hg38")) {
			chr = "chr" + output.getChromosome();
		} else {
			chr = output.getChromosome();
		}

		// set parameters
		Map<String, Object> binding = new HashMap<String, Object>();
		binding.put("ref", refFilename);
		binding.put("vcf", output.getPhasedVcfFilename());
		binding.put("start", output.getStart());
		binding.put("end", output.getEnd());
		binding.put("window", minimacWindow);
		binding.put("prefix", output.getPrefix());
		binding.put("chr", chr);
		binding.put("unphased", false);
		binding.put("mapMinimac", mapMinimac);

		String[] params = createParams(minimacParams, binding);

		// minimac command
		Command minimac = new Command(minimacCommand);
		minimac.setSilent(false);
		minimac.setParams(params);
		minimac.saveStdOut(output.getPrefix() + ".minimac.out");
		minimac.saveStdErr(output.getPrefix() + ".minimac.err");

		System.out.println(minimac.getExecutedCommand());

		return (minimac.execute() == 0);

	}

	public void setTabixCommand(String tabixCommand) {
		this.tabixCommand = tabixCommand;
	}

	public void setRefFilename(String refFilename) {
		this.refFilename = refFilename;
	}

	public void setMapMinimac(String mapMinimac) {
		this.mapMinimac = mapMinimac;
	}

	public void setMapEagleFilename(String mapEagleFilename) {
		this.mapEagleFilename = mapEagleFilename;
	}

	public void setRefEagleFilename(String refEagleFilename) {
		this.refEagleFilename = refEagleFilename;
	}

	public void setMinimacCommand(String minimacCommand, String minimacParams) {
		this.minimacCommand = minimacCommand;
		this.minimacParams = minimacParams;
	}

	public void setMinimacWindow(int minimacWindow) {
		this.minimacWindow = minimacWindow;
	}

	public void setEagleCommand(String eagleCommand, String eagleParams) {
		this.eagleCommand = eagleCommand;
		this.eagleParams = eagleParams;
	}

	public void setPhasingWindow(int phasingWindow) {
		this.phasingWindow = phasingWindow;
	}

	public void setBuild(String build) {
		this.build = build;
	}

	public boolean isPhasingOnly() {
		return phasingOnly;
	}

	public void setPhasingOnly(boolean phasingOnly) {
		this.phasingOnly = phasingOnly;
	}

	public ImputationStatistic getStatistic() {
		return statistic;
	}

	protected String[] createParams(String template, Map<String, Object> bindings) throws IOException {
		
		try {
			String outputTemplate = "";
			outputTemplate = engine.createTemplate(template).make(bindings).toString();
			return outputTemplate.split("\\s+");
		} catch (Exception e) {
			throw new IOException(e);
		}

	}

}
