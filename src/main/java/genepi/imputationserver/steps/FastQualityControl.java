package genepi.imputationserver.steps;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.HashSet;

import cloudgene.sdk.internal.WorkflowContext;
import cloudgene.sdk.internal.WorkflowStep;
import genepi.imputationserver.steps.fastqc.ITask;
import genepi.imputationserver.steps.fastqc.ITaskProgressListener;
import genepi.imputationserver.steps.fastqc.LiftOverTask;
import genepi.imputationserver.steps.fastqc.RangeEntry;
import genepi.imputationserver.steps.fastqc.StatisticsTask;
import genepi.imputationserver.steps.fastqc.TaskResults;
import genepi.imputationserver.steps.vcf.VcfFileUtil;
import genepi.imputationserver.util.DefaultPreferenceStore;
import genepi.imputationserver.util.RefPanel;
import genepi.imputationserver.util.RefPanelList;
import genepi.io.FileUtil;
import genepi.io.text.LineWriter;

public class FastQualityControl extends WorkflowStep {

	protected void setupTabix(String folder) {
		VcfFileUtil.setTabixBinary(FileUtil.path(folder, "bin", "tabix"));
	}

	@Override
	public boolean run(WorkflowContext context) {

		String folder = getFolder(FastQualityControl.class);
		setupTabix(folder);
		String inputFiles = context.get("files");
		String reference = context.get("refpanel");
		String population = context.get("population");

		String mafFile = context.get("mafFile");
		String chunkFileDir = context.get("chunkFileDir");
		String statDir = context.get("statisticDir");
		String chunksDir = context.get("chunksDir");
		String buildGwas = context.get("build");

		// set default build
		if (buildGwas == null) {
			buildGwas = "hg19";
		}

		// load job.config
		File jobConfig = new File(FileUtil.path(folder, "job.config"));
		DefaultPreferenceStore store = new DefaultPreferenceStore();
		if (jobConfig.exists()) {
			store.load(jobConfig);
		} else {
			context.log("Configuration file '" + jobConfig.getAbsolutePath() + "' not available. Use default values.");
		}
		int phasingWindow = Integer.parseInt(store.getString("phasing.window"));
		int chunkSize = Integer.parseInt(store.getString("chunksize"));

		// load reference panels
		RefPanelList panels = RefPanelList.loadFromFile(FileUtil.path(folder, RefPanelList.FILENAME));

		// check reference panel
		RefPanel panel = null;
		try {
			panel = panels.getById(reference, context.getData("refpanel"));
			if (panel == null) {
				context.error("Reference '" + reference + "' not found.");
				context.error("Available references:");
				for (RefPanel p : panels.getPanels()) {
					context.error(p.getId());
				}

				return false;
			}
		} catch (Exception e) {
			context.error("Unable to parse reference panel '" + reference + "': " + e.getMessage());
			return false;
		}

		String[] vcfFilenames = FileUtil.getFiles(inputFiles, "*.vcf.gz$|*.vcf$");

		Arrays.sort(vcfFilenames);

		LineWriter excludedSnpsWriter = null;

		String excludedSnpsFile = FileUtil.path(statDir, "snps-excluded.txt");

		try {
			excludedSnpsWriter = new LineWriter(excludedSnpsFile);
			excludedSnpsWriter.write("#Position" + "\t" + "FilterType" + "\t" + " Info", false);
		} catch (Exception e) {
			context.error("Error creating file writer");
			return false;
		}

		// check if liftover is needed
		if (!buildGwas.equals(panel.getBuild())) {
			context.warning("Uploaded data is " + buildGwas + " and reference is " + panel.getBuild() + ".");
			String chainFile = store.getString(buildGwas + "To" + panel.getBuild());
			if (chainFile == null) {
				context.error("Currently we do not support liftOver from " + buildGwas + " to " + panel.getBuild());
				return false;
			}

			String fullPathChainFile = FileUtil.path(folder, chainFile);
			if (!new File(fullPathChainFile).exists()) {
				context.error("Chain file " + fullPathChainFile + " not found.");
				return false;
			}

			LiftOverTask task = new LiftOverTask();
			task.setVcfFilenames(vcfFilenames);
			task.setChainFile(fullPathChainFile);
			task.setChunksDir(chunksDir);
			task.setExcludedSnpsWriter(excludedSnpsWriter);

			TaskResults results = runTask(context, task);

			if (results.isSuccess()) {
				vcfFilenames = task.getNewVcfFilenames();
			} else {
				return false;
			}

		}

		// calculate statistics
		StatisticsTask task = new StatisticsTask();
		task.setVcfFilenames(vcfFilenames);
		task.setExcludedSnpsWriter(excludedSnpsWriter);
		task.setChunkSize(chunkSize);
		task.setPhasingWindow(phasingWindow);
		task.setPopulation(population);
		// support relative path
		String legend = panel.getLegend();
		if (!legend.startsWith("/")) {
			legend = FileUtil.path(folder, legend);
		}

		// check chromosomes

		if (!panel.supportsPopulation(population)) {
			StringBuilder report = new StringBuilder();
			report.append("Population '" + population + "' is not supported by reference panel '" + reference + "'.\n");
			if (panel.getPopulations() != null) {
				report.append("Available populations:");
				for (String pop : panel.getPopulations().values()) {
					report.append("\n - " + pop);
				}
			}
			context.error(report.toString());
			return false;
		}

		int refSamples = panel.getSamplesByPopulation(population);
		if (refSamples <= 0) {
			context.warning("Skip allele frequency check.");
			task.setAlleleFrequencyCheck(false);
		}

		task.setLegendFile(legend);
		task.setRefSamples(refSamples);
		task.setMafFile(mafFile);
		task.setChunkFileDir(chunkFileDir);
		task.setChunksDir(chunksDir);
		task.setStatDir(statDir);
		task.setBuild(panel.getBuild());

		double referenceOverlap = panel.getQcFilterByKey("overlap");
		int minSnps = (int) panel.getQcFilterByKey("minSnps");
		double sampleCallrate = panel.getQcFilterByKey("sampleCallrate");
		double mixedGenotypesChrX = panel.getQcFilterByKey("mixedGenotypeschrX");
		int strandFlips = (int) (panel.getQcFilterByKey("strandFlips"));
		String ranges = panel.getRange();

		if (ranges != null) {
			HashSet<RangeEntry> rangeEntries = new HashSet<RangeEntry>();

			for (String range : panel.getRange().split(",")) {
				String chromosome = range.split(":")[0].trim();
				String region = range.split(":")[1].trim();
				int start = Integer.valueOf(region.split("-")[0].trim());
				int end = Integer.valueOf(region.split("-")[1].trim());
				RangeEntry entry = new RangeEntry();
				entry.setChromosome(chromosome);
				entry.setStart(start);
				entry.setEnd(end);
				rangeEntries.add(entry);
			}

			task.setRanges(rangeEntries);
			context.log("Reference Panel Ranges: " + rangeEntries);
		} else {
			context.log("Reference Panel Ranges: genome-wide");
		}

		task.setReferenceOverlap(referenceOverlap);
		task.setMinSnps(minSnps);
		task.setSampleCallrate(sampleCallrate);
		task.setMixedGenotypeschrX(mixedGenotypesChrX);

		TaskResults results = runTask(context, task);

		if (!results.isSuccess()) {
			return false;
		}

		try {

			excludedSnpsWriter.close();

			if (!excludedSnpsWriter.hasData()) {
				FileUtil.deleteFile(excludedSnpsFile);
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		DecimalFormat df = new DecimalFormat("#.00");
		DecimalFormat formatter = new DecimalFormat("###,###.###");

		StringBuffer text = new StringBuffer();

		text.append("<b>Statistics:</b> <br>");
		if (ranges != null) {
			text.append("Ref. Panel Range: " + ranges + "<br>");
		}
		text.append(
				"Alternative allele frequency > 0.5 sites: " + formatter.format(task.getAlternativeAlleles()) + "<br>");
		text.append("Reference Overlap: "
				+ df.format(
						task.getFoundInLegend() / (double) (task.getFoundInLegend() + task.getNotFoundInLegend()) * 100)
				+ " %" + "<br>");

		text.append("Match: " + formatter.format(task.getMatch()) + "<br>");
		text.append("Allele switch: " + formatter.format(task.getAlleleSwitch()) + "<br>");
		text.append("Strand flip: " + formatter.format(task.getStrandFlipSimple()) + "<br>");
		text.append("Strand flip and allele switch: " + formatter.format(task.getStrandFlipAndAlleleSwitch()) + "<br>");
		text.append("A/T, C/G genotypes: " + formatter.format(task.getComplicatedGenotypes()) + "<br>");

		text.append("<b>Filtered sites:</b> <br>");
		text.append("Filter flag set: " + formatter.format(task.getFilterFlag()) + "<br>");
		text.append("Invalid alleles: " + formatter.format(task.getInvalidAlleles()) + "<br>");
		text.append("Multiallelic sites: " + formatter.format(task.getMultiallelicSites()) + "<br>");
		text.append("Duplicated sites: " + formatter.format(task.getDuplicates()) + "<br>");
		text.append("NonSNP sites: " + formatter.format(task.getNoSnps()) + "<br>");
		text.append("Monomorphic sites: " + formatter.format(task.getMonomorphic()) + "<br>");
		text.append("Allele mismatch: " + formatter.format(task.getAlleleMismatch()) + "<br>");
		text.append("SNPs call rate < 90%: " + formatter.format(task.getLowCallRate()));

		context.ok(text.toString());

		text = new StringBuffer();

		text.append("Excluded sites in total: " + formatter.format(task.getFiltered()) + "<br>");
		text.append("Remaining sites in total: " + formatter.format(task.getOverallSnps()) + "<br>");

		if (task.getFiltered() > 0) {
			text.append(
					"See " + context.createLinkToFile("statisticDir", "snps-excluded.txt") + " for details" + "<br>");
		}

		if (task.getNotFoundInLegend() > 0) {
			text.append("Typed only sites: " + formatter.format(task.getNotFoundInLegend()) + "<br>");
			text.append("See " + context.createLinkToFile("statisticDir", "typed-only.txt") + " for details" + "<br>");
		}

		if (task.getRemovedChunksSnps() > 0) {

			text.append("<br><b>Warning:</b> " + formatter.format(task.getRemovedChunksSnps())

					+ " Chunk(s) excluded: < " + minSnps + " SNPs (see "
					+ context.createLinkToFile("statisticDir", "chunks-excluded.txt") + "  for details).");
		}

		if (task.getRemovedChunksCallRate() > 0) {

			text.append("<br><b>Warning:</b> " + formatter.format(task.getRemovedChunksCallRate())

					+ " Chunk(s) excluded: at least one sample has a call rate < " + (sampleCallrate * 100) + "% (see "
					+ context.createLinkToFile("statisticDir", "chunks-excluded.txt") + " for details).");
		}

		if (task.getRemovedChunksOverlap() > 0) {

			text.append("<br><b>Warning:</b> " + formatter.format(task.getRemovedChunksOverlap())

					+ " Chunk(s) excluded: reference overlap < " + (referenceOverlap * 100) + "% (see "
					+ context.createLinkToFile("statisticDir", "chunks-excluded.txt") + " for details).");
		}

		long excludedChunks = task.getRemovedChunksSnps() + task.getRemovedChunksCallRate()
				+ task.getRemovedChunksOverlap();

		long overallChunks = task.getOverallChunks();

		if (excludedChunks > 0) {
			text.append("<br>Remaining chunk(s): " + formatter.format(overallChunks - excludedChunks));

		}

		if (excludedChunks == overallChunks) {

			text.append("<br><b>Error:</b> No chunks passed the QC step. Imputation cannot be started!");
			context.error(text.toString());

			return false;

		}
		// strand flips (normal flip & allele switch + strand flip)
		else if (task.getStrandFlipSimple() + task.getStrandFlipAndAlleleSwitch() > strandFlips) {
			text.append("<br><b>Error:</b> More than " + strandFlips
					+ " obvious strand flips have been detected. Please check strand. Imputation cannot be started!");
			context.error(text.toString());

			return false;
		}

		else if (task.isChrXMissingRate()) {
			text.append(
					"<br><b>Error:</b> Chromosome X nonPAR region includes > 10 % mixed genotypes. Imputation cannot be started!");
			context.error(text.toString());

			return false;
		}

		else if (task.isChrXPloidyError()) {
			text.append(
					"<br><b>Error:</b> ChrX nonPAR region includes ambiguous samples (haploid and diploid positions). Imputation cannot be started! See "
							+ context.createLinkToFile("statisticDir", "chrX-info.txt"));
			context.error(text.toString());

			return false;
		}

		else {

			text.append(results.getMessage());
			context.warning(text.toString());
			return true;

		}

	}

	protected TaskResults runTask(final WorkflowContext context, ITask task) {
		context.beginTask("Running " + task.getName() + "...");
		TaskResults results;
		try {
			results = task.run(new ITaskProgressListener() {

				@Override
				public void progress(String message) {
					context.updateTask(message, WorkflowContext.RUNNING);

				}
			});

			if (results.isSuccess()) {
				context.endTask(task.getName(), WorkflowContext.OK);
			} else {
				context.endTask(task.getName() + "\n" + results.getMessage(), WorkflowContext.ERROR);
			}
			return results;
		} catch (InterruptedException e) {
			e.printStackTrace();
			TaskResults result = new TaskResults();
			result.setSuccess(false);
			result.setMessage(e.getMessage());
			StringWriter s = new StringWriter();
			e.printStackTrace(new PrintWriter(s));
			context.println("Task '" + task.getName() + "' failed.\nException:" + s.toString());
			context.endTask(e.getMessage(), WorkflowContext.ERROR);
			return result;
		} catch (Exception e) {
			e.printStackTrace();
			TaskResults result = new TaskResults();
			result.setSuccess(false);
			result.setMessage(e.getMessage());
			StringWriter s = new StringWriter();
			e.printStackTrace(new PrintWriter(s));
			context.println("Task '" + task.getName() + "' failed.\nException:" + s.toString());
			context.endTask(task.getName() + " failed.", WorkflowContext.ERROR);
			return result;
		} catch (Error e) {
			e.printStackTrace();
			TaskResults result = new TaskResults();
			result.setSuccess(false);
			result.setMessage(e.getMessage());
			StringWriter s = new StringWriter();
			e.printStackTrace(new PrintWriter(s));
			context.println("Task '" + task.getName() + "' failed.\nException:" + s.toString());
			context.endTask(task.getName() + " failed.", WorkflowContext.ERROR);
			return result;
		}

	}

}
