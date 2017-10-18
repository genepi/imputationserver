package genepi.imputationserver.steps.vcf;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Vector;

import genepi.imputationserver.steps.vcf.sort.VcfLine;
import genepi.imputationserver.steps.vcf.sort.VcfLineSortingCollection;
import genepi.io.text.LineReader;
import htsjdk.samtools.liftover.LiftOver;
import htsjdk.samtools.util.Interval;
import htsjdk.samtools.util.SortingCollection;

public class VcfLiftOverFast {

	private static final int MAX_RECORDS_IN_RAM = 1000;

	public static Vector<String> liftOver(String input, String output, String chainFile, String tempDir)
			throws IOException {

		LineReader reader = new LineReader(input);

		LiftOver liftOver = new LiftOver(new File(chainFile));

		Vector<String> errors = new Vector<String>();

		SortingCollection<VcfLine> sorter = VcfLineSortingCollection.newInstance(MAX_RECORDS_IN_RAM, tempDir);

		BGzipLineWriter writer = new BGzipLineWriter(output);
		while (reader.next()) {
			String line = reader.get();
			if (line.startsWith("#")) {
				writer.write(line);
			} else {

				VcfLine vcfLine = new VcfLine(line);
				String contig = "";
				if (vcfLine.getContig().startsWith("chr")){
					contig = vcfLine.getContig();
				}else{
					contig = "chr" + vcfLine.getContig();					
				}
				Interval source = new Interval(contig, vcfLine.getPosition(),
						vcfLine.getPosition() + 1, false, vcfLine.getContig() + ":" + vcfLine.getPosition());
				Interval target = liftOver.liftOver(source);
				if (target != null) {
					if (source.getContig().equals(target.getContig())) {
						vcfLine.setContig(target.getContig());
						vcfLine.setPosition(target.getStart());
						sorter.add(vcfLine);
					} else {
						errors.add(vcfLine.getContig() + ":" + vcfLine.getPosition() + "\t" + "LiftOver" + "\t"
								+ "On different chromosome after LiftOver. SNP removed.");
					}
				} else {
					errors.add(vcfLine.getContig() + ":" + vcfLine.getPosition() + "\t" + "LiftOver" + "\t"
							+ "LiftOver failed. SNP removed.");
				}
			}

		}
		reader.close();
		sorter.doneAdding();

		for (VcfLine vcfLine : sorter) {
			writer.write(vcfLine.getLine());
		}
		writer.close();
		sorter.cleanup();

		// create tabix index
		VcfFileUtil.createIndex(output, true);

		return errors;
	}

	public static void main(String[] args) throws IOException {
		// String input =
		// "/home/lukas/cloud/Genepi/Testdata/imputationserver/chr20.R50.merged.1.330k.recode.vcf.gz";
		// String output =
		// "/home/lukas/cloud/Genepi/Testdata/imputationserver/chr20.R50.merged.1.330k.recode.hg38.vcf.gz";

		VcfFileUtil.setTabixBinary("files/minimac/bin/tabix");
		
		String input = "/home/lukas/git/imputationserver-public2/test-data/data/big/chr1-wrayner-filtered-reheader.vcf.gz";
		String output = "lf.hg38.vcf.gz";

		String chainFile = "files/minimac/hg19ToHg38.over.chain.gz";
		long start = System.currentTimeMillis();
		List<String> errors = VcfLiftOverFast.liftOver(input, output, chainFile, "./temp");
		long end = System.currentTimeMillis();
		VcfFile fileInput = VcfFileUtil.load(input, 1000000, false);
		VcfFile fileOutput = VcfFileUtil.load(output, 1000000, false);

		if (fileInput.getNoSamples() == fileOutput.getNoSamples()) {
			System.out.println("Samples are okey");
		} else {
			System.out.println("Different Samples: " + fileInput.getNoSamples() + " vs. " + fileOutput.getNoSamples());
		}
		if (fileInput.getNoSnps() == fileOutput.getNoSnps() + errors.size()) {
			System.out.println("Snps are okey");
		} else {
			System.out.println("Different snps: " + fileInput.getNoSnps() + " vs. " + fileOutput.getNoSnps()
					+ " (not lifted: " + errors.size() + ")");
		}

		System.out.println("LiftOver time: " + (end - start) / 1000 + " sec");

	}

}
