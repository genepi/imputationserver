package genepi.imputationserver.steps.imputationMinimac3;

import genepi.hadoop.command.Command;
import genepi.imputationserver.steps.vcf.VcfChunk;
import genepi.imputationserver.steps.vcf.VcfChunkOutput;
import genepi.io.FileUtil;
import genepi.io.plink.MapFileReader;
import genepi.io.plink.Snp;
import genepi.io.text.LineReader;
import genepi.io.text.LineWriter;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

public class ImputationPipelineMinimac3 {

	private String minimacCommand;
	private String hapiUrCommand;
	private String hapiUrPreprocessCommand;
	private String shapeItCommand;
	private String vcfCookerCommand;
	private String vcf2HapCommand;
	private String refPanelFilename;
	private int minimacWindow;
	private int phasingWindow;
	private int rounds;
	private String mapFilename;

	private String refFilename;
	private String pattern;

	private String mapShapeITPattern;
	private String mapShapeITFilename;

	private String mapHapiURFilename;
	private String mapHapiURPattern;

	private String phasing;

	public String getRefPanelFilename() {
		return refPanelFilename;
	}

	public void setRefPanelFilename(String refPanelFilename) {
		this.refPanelFilename = refPanelFilename;
	}

	public String getRefFilename() {
		return refFilename;
	}

	public void setRefFilename(String refFilename) {
		this.refFilename = refFilename;
	}

	public String getPattern() {
		return pattern;
	}

	public void setPattern(String pattern) {
		this.pattern = pattern;
	}

	public String getMapShapeITPattern() {
		return mapShapeITPattern;
	}

	public void setMapShapeITPattern(String mapShapeITPattern) {
		this.mapShapeITPattern = mapShapeITPattern;
	}

	public String getMapShapeITFilename() {
		return mapShapeITFilename;
	}

	public void setMapShapeITFilename(String mapShapeITFilename) {
		this.mapShapeITFilename = mapShapeITFilename;
	}

	public String getMapHapiURFilename() {
		return mapHapiURFilename;
	}

	public void setMapHapiURFilename(String mapHapiURFilename) {
		this.mapHapiURFilename = mapHapiURFilename;
	}

	public String getMapHapiURPattern() {
		return mapHapiURPattern;
	}

	public void setMapHapiURPattern(String mapHapiURPattern) {
		this.mapHapiURPattern = mapHapiURPattern;
	}

	public String getPhasing() {
		return phasing;
	}

	public void setPhasing(String phasing) {
		this.phasing = phasing;
	}

	public void setMinimacCommand(String minimacCommand) {
		this.minimacCommand = minimacCommand;
	}

	public void setHapiUrCommand(String hapiUrCommand) {
		this.hapiUrCommand = hapiUrCommand;
	}

	public void setVcfCookerCommand(String vcfCookerCommand) {
		this.vcfCookerCommand = vcfCookerCommand;
	}

	public void setVcf2HapCommand(String vcf2HapCommand) {
		this.vcf2HapCommand = vcf2HapCommand;
	}

	public void setShapeItCommand(String shapeItCommand) {
		this.shapeItCommand = shapeItCommand;
	}

	public void setMinimacWindow(int minimacWindow) {
		this.minimacWindow = minimacWindow;
	}

	public void setPhasingWindow(int phasingWindow) {
		this.phasingWindow = phasingWindow;
	}

	public void setReferencePanel(String refPanelFilename) {
		this.refPanelFilename = refPanelFilename;
	}

	public int getRounds() {
		return rounds;
	}

	public void setRounds(int rounds) {
		this.rounds = rounds;
	}

	public boolean execute(VcfChunk chunk, VcfChunkOutput output)
			throws InterruptedException, IOException {

		System.out.println("Starting pipeline for chunk " + chunk + "...");

		String chrFilename = "";

		if (chunk.getChromosome().contains("X.no.auto")) {

			chrFilename = pattern.replaceAll("\\$chr", "X.no.auto");
		} else if (chunk.getChromosome().contains("X.auto")) {

			chrFilename = pattern.replaceAll("\\$chr", "X.auto");
		} else {
			chrFilename = pattern.replaceAll("\\$chr", chunk.getChromosome());
		}

		String refPanelFilename = FileUtil.path(refFilename, chrFilename);

		if (!new File(refPanelFilename).exists()) {
			System.out.println("ReferencePanel '" + refPanelFilename
					+ "' not found.");
			return false;
		}

		setReferencePanel(refPanelFilename);

		if (chunk.isPhased()) {

			// replace X.nonpar / X.par with X
			if (chunk.getChromosome().contains("X")) {
				chunk.setChromosome("X");
				output.setChromosome("X");
			}

			FileUtil.copy(output.getVcfFilename(),
					output.getPhasedVcfFilename());

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

			// convert vcf to bim/bed/fam
			long time = System.currentTimeMillis();
			boolean successful = vcfToBed(output);

			if (chunk.getChromosome().equals("X.no.auto_male")) {
				writeMaleFam(output);
			}

			// replace X.nonpar / X.par with X
			if (chunk.getChromosome().contains("X")) {
				chunk.setChromosome("X");
				output.setChromosome("X");
			}

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
				System.out.println("  Chunk " + chunk + " has only " + noSnps
						+ " markers. Ignore it.");
				return false;
			} else {
				System.out.println("  Before imputation: " + noSnps + " SNPs");
			}

			// phasing
			if (!phasing.equals("shapeit")) {

				// hapiur
				time = System.currentTimeMillis();

				chrFilename = mapHapiURPattern.replaceAll("\\$chr",
						chunk.getChromosome());
				String mapfilePath = FileUtil.path(mapHapiURFilename,
						chrFilename);

				if (!new File(mapfilePath).exists()) {
					System.out.println("Map '" + mapfilePath + "' not found.");
					return false;
				}

				setMapFilename(mapfilePath);

				successful = phaseWithHapiUr(chunk, output);
				time = (System.currentTimeMillis() - time) / 1000;

				if (successful) {
					System.out
							.println("  HapiUR successful [" + time + " sec]");
				} else {
					System.out.println("  HapiUR failed[" + time + " sec]");
					return false;
				}

			} else {

				// shapeit

				chrFilename = mapShapeITPattern.replaceAll("\\$chr",
						chunk.getChromosome());
				String mapfilePath = FileUtil.path(mapShapeITFilename,
						chrFilename);

				if (!new File(mapfilePath).exists()) {
					System.out.println("Map '" + mapfilePath + "' not found.");
					return false;
				}

				setMapFilename(mapfilePath);

				time = System.currentTimeMillis();
				successful = phaseWithShapeIt(chunk, output, chunk
						.getChromosome().equals("X"));
				time = (System.currentTimeMillis() - time) / 1000;

				if (successful) {
					System.out.println("  ShapeIt successful. [" + time
							+ " sec]");
				} else {
					System.out.println("  ShapeIt failed [" + time + " sec]");
					return false;
				}

			}

			time = System.currentTimeMillis();
			successful = imputeVCF(output);
			time = (System.currentTimeMillis() - time) / 1000;

			if (successful) {
				System.out.println("  Minimac3 successful.[" + time + " sec]");
				return true;
			} else {
				String stdOut = FileUtil.readFileAsString(output.getPrefix()
						+ ".minimac.out");
				String stdErr = FileUtil.readFileAsString(output.getPrefix()
						+ ".minimac.err");

				System.out.println("  Minimac3 failed[" + time
						+ " sec]\n\nStdOut:\n" + stdOut + "\nStdErr:\n"
						+ stdErr);
				return false;
			}
		}
	}

	public boolean vcfToBed(VcfChunkOutput output) {

		Command vcfCooker = new Command(vcfCookerCommand);
		vcfCooker.setSilent(true);

		vcfCooker.setParams("--in-vcf", output.getVcfFilename(), "--write-bed",
				"--out", output.getPrefix());
		vcfCooker.saveStdOut(output.getPrefix() + ".vcfcooker.out");
		vcfCooker.saveStdErr(output.getPrefix() + ".vcfcooker.err");
		System.out.println("Command: " + vcfCooker.getExecutedCommand());
		return (vcfCooker.execute() == 0);

	}

	public void writeMaleFam(VcfChunkOutput output) {

		try {
			FileUtils.copyFile(new File(output.getFamFilename()), new File(
					output.getFamFilename() + "_tmp"));
			LineReader reader = new LineReader(output.getFamFilename() + "_tmp");
			LineWriter writer = new LineWriter(output.getFamFilename());
			while (reader.next()) {
				String str = "";
				String[] token = reader.get().split("\\s+");
				// set male as gender
				token[4] = "1";
				for (String i : token) {
					str += i + "\t";
				}
				writer.write(str);

			}
			writer.close();
			reader.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public boolean vcfToHap2(VcfChunkOutput output) {
		Command vcf2Hap = new Command(vcf2HapCommand);
		vcf2Hap.setSilent(false);
		vcf2Hap.setParams("--in-vcf", output.getVcfFilename(), "--out",
				output.getPrefix());
		vcf2Hap.saveStdOut(output.getPrefix() + ".vcf2hap.out");
		vcf2Hap.saveStdErr(output.getPrefix() + ".vcf2hap.err");
		return (vcf2Hap.execute() == 0);
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
				System.out.println(snp.getPhysicalPosition());
				if (snp.getPhysicalPosition() >= input.getStart()
						&& snp.getPhysicalPosition() <= input.getEnd()) {
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

	public boolean phaseWithHapiUr(VcfChunk input, VcfChunkOutput output) {

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

		hapiUr.setParams(
				"-g",
				output.getBedFilename(),
				"-s",
				bimWthMap,
				"-i",
				output.getFamFilename(),
				"-w",
				"73",
				"-o",
				output.getPrefix(),
				"-c",
				input.getChromosome().equals("X") ? "23" : input
						.getChromosome(), "--start", start + "", "--end", end
						+ "", "--impute2");
		hapiUr.saveStdOut(output.getPrefix() + ".hapiur.out");
		hapiUr.saveStdErr(output.getPrefix() + ".hapiur.err");
		System.out.println("Command: " + hapiUr.getExecutedCommand());
		if (hapiUr.execute() != 0) {
			return false;
		}

		// haps to vcf
		Command shapeItConvert = new Command(shapeItCommand);
		shapeItConvert.setSilent(false);
		shapeItConvert.setParams("-convert", "--input-haps",
				output.getPrefix(), "--output-vcf",
				output.getPhasedVcfFilename());
		System.out.println("Command: " + shapeItConvert.getExecutedCommand());

		if (shapeItConvert.execute() != 0) {
			return false;
		}

		if (input.getChromosome().equals("X")) {
			replace23WithX(output.getPhasedVcfFilename());
		}

		return true;
	}

	public boolean phaseWithShapeIt(VcfChunk input, VcfChunkOutput output,
			boolean chrX) {

		// +/- 1 Mbases
		int start = input.getStart() - phasingWindow;
		if (start < 1) {
			start = 1;
		}

		int end = input.getEnd() + phasingWindow;

		Command shapeIt = new Command(shapeItCommand);
		shapeIt.setSilent(false);
		if (chrX) {
			shapeIt.setParams("--input-bed", output.getBedFilename(),
					output.getBimFilename(), output.getFamFilename(),
					"--input-map", mapFilename, "--output-max",
					output.getPrefix(), "--input-from", start + "",
					"--input-to", end + "", "--chrX", "--effective-size",
					"11418");
		} else {
			shapeIt.setParams("--input-bed", output.getBedFilename(),
					output.getBimFilename(), output.getFamFilename(),
					"--input-map", mapFilename, "--output-max",
					output.getPrefix(), "--input-from", start + "",
					"--input-to", end + "", "--effective-size", "11418");
		}
		shapeIt.saveStdOut(output.getPrefix() + ".shapeit.out");
		shapeIt.saveStdErr(output.getPrefix() + ".shapeit.err");
		System.out.println("Command: " + shapeIt.getExecutedCommand());
		if (shapeIt.execute() != 0) {
			return false;
		}

		// haps to vcf
		Command shapeItConvert = new Command(shapeItCommand);
		shapeItConvert.setSilent(false);
		shapeItConvert.setParams("-convert", "--input-haps",
				output.getPrefix(), "--output-vcf",
				output.getPhasedVcfFilename());
		System.out.println("Command: " + shapeItConvert.getExecutedCommand());
		if (shapeItConvert.execute() != 0) {
			return false;
		}

		if (chrX) {
			replace23WithX(output.getPhasedVcfFilename());
		}

		return true;
	}

	public boolean imputeVCF(VcfChunkOutput output)
			throws InterruptedException, IOException {

		if (output.getPhasedVcfFilename().contains("no.auto_male")) {
			replaceMale(output.getPhasedVcfFilename());
		}

		// mini-mac
		Command minimac = new Command(minimacCommand);
		minimac.setSilent(true);
		minimac.setParams("--refHaps", refPanelFilename, "--haps",
				output.getPhasedVcfFilename(), "--rounds", rounds + "",
				"--start", output.getStart() + "", "--end", output.getEnd()
						+ "", "--window", minimacWindow + "", "--prefix",
				output.getPrefix(), "--chr", output.getChromosome(),
				"--noPhoneHome");//, "--format", "GT,DS,GP");

		minimac.saveStdOut(output.getPrefix() + ".minimac.out");
		minimac.saveStdErr(output.getPrefix() + ".minimac.err");

		System.out.println(minimac.getExecutedCommand());

		return (minimac.execute() == 0);

	}

	public int fixInfoFile(VcfChunk input, VcfChunkOutput output)
			throws IOException, InterruptedException {

		// fix window bug in minimac

		LineReader readerInfo = new LineReader(output.getInfoFilename());
		LineWriter writerInfo = new LineWriter(output.getInfoFixedFilename());

		readerInfo.next();
		String header = readerInfo.get();
		writerInfo.write(header);

		int startIndex = Integer.MAX_VALUE;
		int endIndex = 0;

		int index = 0;
		int snps = 0;
		while (readerInfo.next()) {
			String line = readerInfo.get();
			String[] tiles = line.split("\t", 2);
			int position = Integer.parseInt(tiles[0].split(":")[1]);
			if (position >= input.getStart() && position <= input.getEnd()) {
				startIndex = Math.min(startIndex, index);
				endIndex = Math.max(endIndex, index);
				writerInfo.write(line);
				snps++;
			}
			index++;
		}

		readerInfo.close();
		writerInfo.close();

		return snps;

	}

	public boolean replace23WithX(String filename) {
		try {
			String tempFilename = filename + "_temp";
			LineWriter writer = new LineWriter(tempFilename);
			LineReader reader = new LineReader(filename);
			while (reader.next()) {

				String line = reader.get();

				if (line.startsWith("#")) {
					writer.write(line);
				} else {
					String[] tiles = line.split("\t", 2);
					writer.write("X\t" + tiles[1]);
				}

			}
			reader.close();
			writer.close();
			FileUtil.deleteFile(filename);
			FileUtil.copy(tempFilename, filename);
			FileUtil.deleteFile(tempFilename);
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	public boolean replaceMale(String filename) {

		try {
			String tempFilename = filename + "_temp";
			LineReader reader = new LineReader(filename);
			LineWriter writer = new LineWriter(tempFilename);
			while (reader.next()) {

				String line = reader.get();
				if (line.startsWith("#")) {
					writer.write(line);
				} else {
					String tiles[] = line.split("\t", 10);
					tiles[9] = tiles[9].replaceAll("0\\|0", "0").replaceAll(
							"1\\|1", "1");
					StringBuffer result = new StringBuffer();
					for (int i = 0; i < tiles.length; i++) {
						result.append(tiles[i] + "\t");
					}
					writer.write(result.toString());
				}
			}
			reader.close();
			writer.close();
			FileUtil.deleteFile(filename);
			FileUtil.copy(tempFilename, filename);
			FileUtil.deleteFile(tempFilename);
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	public String getHapiUrPreprocessCommand() {
		return hapiUrPreprocessCommand;
	}

	public void setHapiUrPreprocessCommand(String hapiUrPreprocessCommand) {
		this.hapiUrPreprocessCommand = hapiUrPreprocessCommand;
	}

	public String getMapFilename() {
		return mapFilename;
	}

	public void setMapFilename(String mapFilename) {
		this.mapFilename = mapFilename;
	}

}
