package genepi.imputationserver.steps.imputation;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.io.FileUtils;
import org.codehaus.groovy.control.CompilationFailedException;

import genepi.hadoop.command.Command;
import genepi.imputationserver.steps.vcf.VcfChunk;
import genepi.imputationserver.steps.vcf.VcfChunkOutput;
import genepi.io.FileUtil;
import groovy.text.SimpleTemplateEngine;

public class ImputationPipeline {

	private String minimacCommand;
	private String minimacParams;
	private String eagleCommand;
	private String tabixCommand;
	private int minimacWindow;
	private int phasingWindow;

	private String refFilename;

	private String mapMinimac;

	private String mapEagleFilename = "";
	private String refEagleFilename = "";

	private String build = "hg19";
	private boolean phasingOnly;

	public static final String PIPELINE_VERSION = "michigan-imputationserver-1.2.4";
	public static final String IMPUTATION_VERSION = "minimac4-1.0.2";
	public static final String PHASING_VERSION = "eagle-2.4";

	private ImputationStatistic statistic = new ImputationStatistic();

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

			if (phasingOnly) {
				return true;
			}

		}

		//Imputation
		
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

	public boolean phaseWithEagle(VcfChunk input, VcfChunkOutput output, String reference, String mapFilename) {

		// +/- 1 Mbases
		int start = input.getStart() - phasingWindow;
		if (start < 1) {
			start = 1;
		}

		int end = input.getEnd() + phasingWindow;

		// start eagle
		Command eagle = new Command(eagleCommand);
		eagle.setSilent(false);

		String phasedPrefix = ".eagle.phased";

		List<String> params = new Vector<String>();
		params.add("--vcfRef");
		params.add(reference);
		params.add("--vcfTarget");
		params.add(output.getVcfFilename());
		params.add("--geneticMapFile");
		params.add(mapFilename);
		params.add("--outPrefix");
		params.add(output.getPrefix() + phasedPrefix);
		params.add("--bpStart");
		params.add(start + "");
		params.add("--bpEnd");
		params.add(end + "");
		params.add("--allowRefAltSwap");
		params.add("--vcfOutFormat");
		params.add("z");
		// params.add("--outputUnphased");
		params.add("--keepMissingPloidyX");

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

		// mini-mac
		Command minimac = new Command(minimacCommand);
		minimac.setSilent(false);

		String chr = "";
		if (build.equals("hg38")) {
			chr = "chr" + output.getChromosome();
		} else {
			chr = output.getChromosome();
		}

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

		SimpleTemplateEngine engine = new SimpleTemplateEngine();
		String outputTemplate = "";
		try {
			outputTemplate = engine.createTemplate(minimacParams).make(binding).toString();
		} catch (Exception e) {
			throw new IOException(e);
		}

		String[] outputTemplateParams = outputTemplate.split("\\s+");

		minimac.setParams(outputTemplateParams);

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

	public void setEagleCommand(String eagleCommand) {
		this.eagleCommand = eagleCommand;
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

}
