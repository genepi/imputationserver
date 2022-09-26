package genepi.imputationserver.steps.ancestry;

import java.io.File;
import java.io.IOException;
import java.util.List;

import genepi.hadoop.HdfsUtil;
import genepi.hadoop.command.Command;
import genepi.imputationserver.steps.vcf.BGzipLineWriter;
import genepi.io.text.LineReader;
import genepi.io.text.LineWriter;

public class TracePipeline {

	public static String STUDY_VCF = "study.vcf.gz";

	public static String STUDY_GENO = "study";

	// TODO: user temp folder from config file!
	public static String OUTPUT_PREFIX = "trace.out";

	public String referenceSite = "reference.site";

	public String referenceRange = "reference.range";

	public String referenceGeno = "reference.geno";

	public String referencePCCoord = "reference.RefPC.coord";

	public String referenceSamples = "reference.samples";

	private String proPcCoord;

	private String populations;

	public void execute(TraceBatch batch) throws IOException {

		extractSamplesFromVcfFiles(batch.getStudyVcf(), batch.getStart(), batch.getEnd(), referenceSite, STUDY_VCF);
		convertToGeno(STUDY_VCF, referenceRange, STUDY_GENO);
		trace(STUDY_GENO + ".geno", referenceGeno, referencePCCoord, batch.getDim(), batch.getDimHigh(), OUTPUT_PREFIX);
		proPcCoord = OUTPUT_PREFIX + ".ProPC.coord";
		populations = "samples.txt";
		predictPopulation(proPcCoord, referencePCCoord, referenceSamples, batch.getDim(), populations);

	}

	protected boolean extractSamplesFromVcfFiles(String vcfFolder, long start, long end, String referenceSites,
			String output) throws IOException {

		// TODO: check how vcf2geno filters: not in range --> set missing? how to check
		// different geno files between study and ref.

		List<String> files = HdfsUtil.getFiles(vcfFolder);
		// TODO: need sort?
		BGzipLineWriter writer = new BGzipLineWriter(output);
		boolean firstFile = true;
		for (String filename : files) {
			LineReader reader = new LineReader(HdfsUtil.open(filename));
			while (reader.next()) {
				String line = reader.get();
				if (line.startsWith("#")) {
					if (firstFile) {
						if (line.toLowerCase().startsWith("#chrom")) {
							String newLine = extractColumns(line, start, end);
							writer.write(newLine);
						} else {
							writer.write(line);
						}
					}
				} else {
					String newLine = extractColumns(line, start, end);
					writer.write(newLine);
				}

			}
			reader.close();
			firstFile = false;
		}
		writer.close();

		Command tabix = new Command((new File("tabix")).getAbsolutePath());
		tabix.setSilent(false);
		tabix.setParams(output);
		System.out.println("Command: " + tabix.getExecutedCommand());
		if (tabix.execute() != 0) {
			throw new IOException("Error during index creation: " + tabix.getStdOut());
		}

		return true;
	}

	protected String extractColumns(String line, long start, long end) {
		String tiles[] = line.split("\t");
		String newLine = "";
		for (int i = 0; i < 9; i++) {
			if (i > 0) {
				newLine += "\t";
			}
			newLine += tiles[i];
		}
		for (int i = 9 + (int) start; ((i <= 9 + (int) end) && i <= tiles.length); i++) {
			newLine += "\t" + tiles[i - 1];
		}
		return newLine;
	}

	protected boolean convertToGeno(String vcf, String range, String output) throws IOException {
		Command vcf2geno = new Command((new File("vcf2geno")).getAbsolutePath());
		vcf2geno.setSilent(false);
		vcf2geno.setParams("--inVcf", vcf, "--rangeFile", range, "--out", output);
		System.out.println("Command: " + vcf2geno.getExecutedCommand());
		if (vcf2geno.execute() != 0) {
			throw new IOException("Error during vcf2geno: " + vcf2geno.getStdOut());
		}
		return true;
	}

	protected boolean createTraceConfig(String configFilename, String studyGeno, String referenceGeno,
			String referencePcCoord, long dim, long dimHigh, String outputPrefix) throws IOException {
		LineWriter writer = new LineWriter(configFilename);
		writer.write("GENO_FILE " + referenceGeno);
		writer.write("STUDY_FILE " + studyGeno);
		writer.write("COORD_FILE " + referencePcCoord);
		writer.write("DIM " + dim);
		writer.write("DIM_HIGH " + dimHigh);
		writer.write("OUT_PREFIX " + outputPrefix);
		writer.close();
		return true;
	}

	protected boolean trace(String studyGeno, String referenceGeno, String referencePcCoord, long dim, long dimHigh,
			String outputPrefix) throws IOException {

		String configFilename = "trace.config";
		createTraceConfig(configFilename, studyGeno, referenceGeno, referencePcCoord, dim, dimHigh, outputPrefix);

		Command trace = new Command((new File("trace")).getAbsolutePath());
		trace.setSilent(false);
		trace.setParams("-p", configFilename);
		System.out.println("Command: " + trace.getExecutedCommand());
		if (trace.execute() != 0) {
			throw new IOException("Error during trace: " + trace.getStdOut());
		}

		return true;
	}

	protected boolean predictPopulation(String studyPcCoord, String referencePcCoord, String referenceSamples, int pcs,
			String output) {

		PopulationPredictor predictor = new PopulationPredictor();
		predictor.setSamplesFile(referenceSamples);
		predictor.setReferenceFile(referencePcCoord);
		predictor.setStudyFile(studyPcCoord);
		predictor.setMaxPcs(pcs);
		predictor.predictPopulation(output);

		return true;

	}

	public String getProPCCoord() {
		return proPcCoord;
	}

	public void setReferenceGeno(String referenceGeno) {
		this.referenceGeno = referenceGeno;
	}

	public void setReferencePCCoord(String referencePCCoord) {
		this.referencePCCoord = referencePCCoord;
	}

	public void setReferenceRange(String referenceRange) {
		this.referenceRange = referenceRange;
	}

	public void setReferenceSite(String referenceSite) {
		this.referenceSite = referenceSite;
	}

	public void setReferenceSamples(String referenceSamples) {
		this.referenceSamples = referenceSamples;
	}

	public String getPopulations() {
		return populations;
	}

}
