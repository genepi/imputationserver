package genepi.imputationserver.steps.imputationMinimac3;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.codehaus.groovy.control.CompilationFailedException;

import genepi.hadoop.command.Command;
import genepi.imputationserver.steps.vcf.VcfChunk;
import genepi.imputationserver.steps.vcf.VcfChunkOutput;
import genepi.imputationserver.util.GenomicTools;
import genepi.io.FileUtil;
import genepi.io.plink.MapFileReader;
import genepi.io.plink.Snp;
import genepi.io.text.LineReader;
import groovy.text.SimpleTemplateEngine;
import htsjdk.samtools.util.BlockCompressedOutputStream;

public class ImputationPipelineMinimac3 {

	private String minimacCommand;
	private String minimacParams;
	private String hapiUrCommand;
	private String hapiUrPreprocessCommand;
	private String shapeItCommand;
	private String eagleCommand;
	private String tabixCommand;
	private String vcfCookerCommand;
	private int minimacWindow;
	private int phasingWindow;
	private int rounds;

	private String refFilename;

	private String mapMinimac;

	private String mapShapeITPattern;
	private String mapShapeITFilename;

	private String mapHapiURFilename;
	private String mapHapiURPattern;

	private String mapEagleFilename = "";
	private String refEagleFilename = "";

	private String population;
	private String phasing;
	private String build = "hg19";
	private boolean phasingOnly;

	public boolean execute(VcfChunk chunk, VcfChunkOutput output) throws InterruptedException, IOException {

		System.out.println("Starting pipeline for chunk " + chunk + " [Phased: " + chunk.isPhased() + "]...");

		if (!new File(refFilename).exists()) {
			System.out.println("ReferencePanel '" + refFilename + "' not found.");
			return false;
		}

		// replace X.nonpar / X.par with X needed by eagle and minimac
		if (chunk.getChromosome().startsWith("X.")) {
			output.setChromosome("X");
		}

		// impute only for phased chromosomes
		if (chunk.isPhased()) {

			FileUtil.copy(output.getVcfFilename(), output.getPhasedVcfFilename());

			long time = System.currentTimeMillis();
			boolean successful = imputeVCF(output);
			time = (System.currentTimeMillis() - time) / 1000;

			if (successful) {
				System.out.println("  Minimac3 successful. [" + time + " sec]");
				return true;
			} else {
				System.out.println("  Minimac3 failed [" + time + " sec]");
				return false;
			}

		} else {

			if (phasing.equals("eagle")) {

				// eagle
				long time = System.currentTimeMillis();

				if (!new File(refEagleFilename).exists()) {
					System.out.println("Eagle: Reference '" + refEagleFilename + "' not found.");
					return false;
				}

				boolean successful = phaseWithEagle(chunk, output, refEagleFilename, mapEagleFilename);
				time = (System.currentTimeMillis() - time) / 1000;

				if (successful) {
					System.out.println("  Eagle successful [" + time + " sec]");
				} else {
					System.out.println("  Eagle failed[" + time + " sec]");
					return false;
				}

			} else {

				// convert vcf to bim/bed/fam
				long time = System.currentTimeMillis();
				boolean successful = vcfToBed(output);

				time = (System.currentTimeMillis() - time) / 1000;

				if (successful) {
					System.out.println("  vcfCooker successful [" + time + " sec]");
				} else {
					System.out.println("  vcfCooker failed[" + time + " sec]");
					return false;
				}

				// ignore small chunks
				int noSnps = getNoSnps(chunk, output);
				if (noSnps <= 2) {
					System.out.println("  Chunk " + chunk + " has only " + noSnps + " markers. Ignore it.");
					return false;
				} else {
					System.out.println("  Before imputation: " + noSnps + " SNPs");
				}

				// phasing
				if (phasing.equals("hapiur")) {

					// hapiur
					time = System.currentTimeMillis();

					String chrFilename = mapHapiURPattern.replaceAll("\\$chr", chunk.getChromosome());
					String mapfilePath = FileUtil.path(mapHapiURFilename, chrFilename);

					if (!new File(mapfilePath).exists()) {
						System.out.println("Map '" + mapfilePath + "' not found.");
						return false;
					}

					successful = phaseWithHapiUr(chunk, output, mapfilePath);
					time = (System.currentTimeMillis() - time) / 1000;

					if (successful) {
						System.out.println("  HapiUR successful [" + time + " sec]");
					} else {
						System.out.println("  HapiUR failed[" + time + " sec]");
						return false;
					}

				} else if (phasing.equals("shapeit")) {

					// shapeit

					String chrFilename = mapShapeITPattern.replaceAll("\\$chr", chunk.getChromosome());
					String mapfilePath = FileUtil.path(mapShapeITFilename, chrFilename);

					if (!new File(mapfilePath).exists()) {
						System.out.println("Map '" + mapfilePath + "' not found.");
						return false;
					}

					time = System.currentTimeMillis();
					successful = phaseWithShapeIt(chunk, output, mapfilePath);
					time = (System.currentTimeMillis() - time) / 1000;

					if (successful) {
						System.out.println("  ShapeIt successful. [" + time + " sec]");
					} else {
						System.out.println("  ShapeIt failed [" + time + " sec]");
						return false;
					}

				}
			}

			if (phasing.equals("eagle") && phasingOnly) {
				return true;
			} else {

				long time = System.currentTimeMillis();
				boolean successful = imputeVCF(output);
				time = (System.currentTimeMillis() - time) / 1000;

				if (successful) {
					System.out.println("  Minimac4 finished successfully.[" + time + " sec]");
					return true;
				} else {
					String stdOut = FileUtil.readFileAsString(output.getPrefix() + ".minimac.out");
					String stdErr = FileUtil.readFileAsString(output.getPrefix() + ".minimac.err");

					System.out.println(
							"  Minimac4 failed[" + time + " sec]\n\nStdOut:\n" + stdOut + "\nStdErr:\n" + stdErr);
					return false;
				}

			}

		}
	}

	public boolean vcfToBed(VcfChunkOutput output) {

		Command vcfCooker = new Command(vcfCookerCommand);
		vcfCooker.setSilent(true);

		vcfCooker.setParams("--in-vcf", output.getVcfFilename(), "--write-bed", "--out", output.getPrefix());
		vcfCooker.saveStdOut(output.getPrefix() + ".vcfcooker.out");
		vcfCooker.saveStdErr(output.getPrefix() + ".vcfcooker.err");
		System.out.println("Command: " + vcfCooker.getExecutedCommand());
		return (vcfCooker.execute() == 0);

	}

	public int getNoSnps(VcfChunk input, VcfChunkOutput output) {
		MapFileReader reader = null;

		try {
			System.out.println(FileUtil.getLineCount(output.getBimFilename()));
		} catch (IOException e1) {
			System.out.println(e1.getMessage());
			e1.printStackTrace();
		}

		int noSnps = 0;
		try {
			reader = new MapFileReader(output.getBimFilename());
			while (reader.next()) {
				Snp snp = reader.get();
				if (snp.getPhysicalPosition() >= input.getStart() && snp.getPhysicalPosition() <= input.getEnd()) {
					noSnps++;
				}
			}
			reader.close();
		} catch (Exception e) {
			System.out.println("Error during snp count: " + e.getMessage());
			e.printStackTrace();
			return -1;
		}
		return noSnps;
	}

	public boolean phaseWithHapiUr(VcfChunk input, VcfChunkOutput output, String mapFilename) {

		// +/- 1 Mbases
		int start = input.getStart() - phasingWindow;
		if (start < 1) {
			start = 1;
		}

		int end = input.getEnd() + phasingWindow;

		String bimWthMap = output.getPrefix() + ".map.bim";

		Command hapiUrPre = new Command(hapiUrPreprocessCommand);
		hapiUrPre.setParams(output.getBimFilename(), mapFilename);
		hapiUrPre.saveStdOut(bimWthMap);
		hapiUrPre.setSilent(true);
		System.out.println("Command: " + hapiUrPre.getExecutedCommand());
		if (hapiUrPre.execute() != 0) {
			return false;
		}

		Command hapiUr = new Command(hapiUrCommand);
		hapiUr.setSilent(false);

		hapiUr.setParams("-g", output.getBedFilename(), "-s", bimWthMap, "-i", output.getFamFilename(), "-w", "73",
				"-o", output.getPrefix(), "-c", input.getChromosome(), "--start", start + "", "--end", end + "",
				"--impute2");
		hapiUr.saveStdOut(output.getPrefix() + ".hapiur.out");
		hapiUr.saveStdErr(output.getPrefix() + ".hapiur.err");
		System.out.println("Command: " + hapiUr.getExecutedCommand());
		if (hapiUr.execute() != 0) {
			return false;
		}

		// haps to vcf
		Command shapeItConvert = new Command(shapeItCommand);
		shapeItConvert.setSilent(false);
		shapeItConvert.setParams("-convert", "--input-haps", output.getPrefix(), "--output-vcf",
				output.getPhasedVcfFilename());
		System.out.println("Command: " + shapeItConvert.getExecutedCommand());

		if (shapeItConvert.execute() != 0) {
			return false;
		}

		return true;
	}

	public boolean phaseWithShapeIt(VcfChunk input, VcfChunkOutput output, String mapFilename) {

		// +/- 1 Mbases
		int start = input.getStart() - phasingWindow;
		if (start < 1) {
			start = 1;
		}

		int end = input.getEnd() + phasingWindow;

		Command shapeIt = new Command(shapeItCommand);
		shapeIt.setSilent(false);

		shapeIt.setParams("--input-bed", output.getBedFilename(), output.getBimFilename(), output.getFamFilename(),
				"--input-map", mapFilename, "--output-max", output.getPrefix(), "--input-from", start + "",
				"--input-to", end + "", "--effective-size", GenomicTools.getPopSize(population) + "");

		shapeIt.saveStdOut(output.getPrefix() + ".shapeit.out");
		shapeIt.saveStdErr(output.getPrefix() + ".shapeit.err");
		System.out.println("Command: " + shapeIt.getExecutedCommand());
		if (shapeIt.execute() != 0) {
			return false;
		}

		// haps to vcf
		Command shapeItConvert = new Command(shapeItCommand);
		shapeItConvert.setSilent(false);
		shapeItConvert.setParams("-convert", "--input-haps", output.getPrefix(), "--output-vcf",
				output.getPhasedVcfFilename());
		System.out.println("Command: " + shapeItConvert.getExecutedCommand());
		if (shapeItConvert.execute() != 0) {
			return false;
		}

		return true;
	}

	public boolean phaseWithEagle(VcfChunk input, VcfChunkOutput output, String reference, String mapFilename) {

		// +/- 1 Mbases
		int start = input.getStart() - phasingWindow;
		if (start < 1) {
			start = 1;
		}

		int end = input.getEnd() + phasingWindow;

		if (!new File(output.getVcfFilename()).exists()) {
			System.out.println("vcf file not created!");
			return false;
		}

		// bgzip
		try {
			boolean first = true;
			LineReader reader = new LineReader(output.getVcfFilename());
			BlockCompressedOutputStream out = new BlockCompressedOutputStream(output.getVcfFilename() + ".gz");
			while (reader.next()) {
				if (!first) {
					out.write("\n".getBytes());
				}
				out.write(reader.get().getBytes());
				first = false;
			}
			reader.close();
			out.close();

		} catch (Exception e) {
			System.out.println("my bgzip failed.");
			e.printStackTrace();
			return false;
		}

		if (!new File(output.getVcfFilename() + ".gz").exists()) {
			System.out.println("vcf.gz file not created!");
			return false;
		}

		// create tabix index
		Command tabix = new Command(tabixCommand);
		tabix.setSilent(false);
		tabix.setParams(output.getVcfFilename() + ".gz");
		System.out.println("Command: " + tabix.getExecutedCommand());
		if (tabix.execute() != 0) {
			System.out.println("Error during index creation: " + tabix.getStdOut());
			return false;
		}

		// start eagle
		Command eagle = new Command(eagleCommand);
		eagle.setSilent(false);

		String phasedPrefix = ".eagle.phased";

		List<String> params = new Vector<String>();
		params.add("--vcfRef");
		params.add(reference);
		params.add("--vcfTarget");
		params.add(output.getVcfFilename() + ".gz");
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
		//params.add("--outputUnphased");
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
		binding.put("unphased", phasing != null && phasing.equals("shapeit") && !output.isPhased());
		binding.put("mapMinimac", mapMinimac);
		binding.put("rounds", rounds);

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

	public void setHapiUrPreprocessCommand(String hapiUrPreprocessCommand) {
		this.hapiUrPreprocessCommand = hapiUrPreprocessCommand;
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

	public void setMapShapeITPattern(String mapShapeITPattern) {
		this.mapShapeITPattern = mapShapeITPattern;
	}

	public void setMapShapeITFilename(String mapShapeITFilename) {
		this.mapShapeITFilename = mapShapeITFilename;
	}

	public void setMapHapiURFilename(String mapHapiURFilename) {
		this.mapHapiURFilename = mapHapiURFilename;
	}

	public void setMapHapiURPattern(String mapHapiURPattern) {
		this.mapHapiURPattern = mapHapiURPattern;
	}

	public void setMapEagleFilename(String mapEagleFilename) {
		this.mapEagleFilename = mapEagleFilename;
	}

	public void setRefEagleFilename(String refEagleFilename) {
		this.refEagleFilename = refEagleFilename;
	}

	public void setPhasing(String phasing) {
		this.phasing = phasing;
	}

	public void setPopulation(String population) {
		this.population = population;
	}

	public void setMinimacCommand(String minimacCommand, String minimacParams) {
		this.minimacCommand = minimacCommand;
		this.minimacParams = minimacParams;
	}

	public void setHapiUrCommand(String hapiUrCommand) {
		this.hapiUrCommand = hapiUrCommand;
	}

	public void setVcfCookerCommand(String vcfCookerCommand) {
		this.vcfCookerCommand = vcfCookerCommand;
	}

	public void setShapeItCommand(String shapeItCommand) {
		this.shapeItCommand = shapeItCommand;
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

	public void setRounds(int rounds) {
		this.rounds = rounds;
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

}
