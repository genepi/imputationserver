package genepi.imputationserver.steps.vcf;

import java.io.File;
import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Vector;

import genepi.io.FileUtil;
import genepi.io.text.LineReader;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.liftover.LiftOver;
import htsjdk.samtools.util.Interval;
import htsjdk.samtools.util.SortingCollection;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.VariantContextComparator;
import htsjdk.variant.variantcontext.writer.Options;
import htsjdk.variant.variantcontext.writer.VariantContextWriter;
import htsjdk.variant.variantcontext.writer.VariantContextWriterBuilder;
import htsjdk.variant.vcf.VCFFileReader;
import htsjdk.variant.vcf.VCFRecordCodec;

public class VcfLiftOver {

	private static final int MAX_RECORDS_IN_RAM = 1000;

	public static Vector<String> liftOver(String input, String output, String chainFile, String tempDir)
			throws IOException {

		LineReader reader = new LineReader(input);
		String outputUnsorted = output + ".unsorted.vcf.gz";
		BGzipLineWriter writer = new BGzipLineWriter(outputUnsorted);

		LiftOver liftOver = new LiftOver(new File(chainFile));

		Vector<String> errors = new Vector<String>();

		while (reader.next()) {
			String line = reader.get();
			if (line.startsWith("#")) {
				writer.write(line);
			} else {

				String[] tiles = line.split("\t", 3);
				Interval source = new Interval("chr" + tiles[0], Integer.parseInt(tiles[1]),
						Integer.parseInt(tiles[1]) + 1, false, tiles[0] + ":" + tiles[1]);
				Interval target = liftOver.liftOver(source);
				if (target != null) {
					if (source.getContig().equals(target.getContig())) {
						writer.write("chr" + tiles[0] + "\t" + target.getStart() + "\t" + tiles[2]);
					} else {
						errors.add(tiles[0] + ":" + tiles[1] + "\t" + "LiftOver" + "\t"
								+ "On different chromosome after LiftOver. SNP removed.");
					}
				} else {
					errors.add(tiles[0] + ":" + tiles[1] + "\t" + "LiftOver" + "\t" + "LiftOver failed. SNP removed.");
				}
			}

		}
		reader.close();
		writer.close();

		// sort new file
		VCFFileReader readerVcf = new VCFFileReader(new File(outputUnsorted), false);

		// fake contigs
		List<String> contigs = new Vector<String>();
		for (int i = 1; i <= 22; i++) {
			contigs.add("chr" + i + "");
		}

		// read from file and add to sorter
		final SortingCollection<VariantContext> sorter = SortingCollection.newInstance(VariantContext.class,
				new VCFRecordCodec(readerVcf.getFileHeader(), true), new VariantContextComparator(contigs),
				MAX_RECORDS_IN_RAM, new File(tempDir));
		for (final VariantContext variantContext : readerVcf) {
			sorter.add(variantContext);
		}
		sorter.doneAdding();

		SAMSequenceDictionary d = readerVcf.getFileHeader().getSequenceDictionary();
		if (d != null) {
			for (String contig : contigs) {
				d.addSequence(new SAMSequenceRecord(contig));
			}
		}

		// write from sorter to file
		final EnumSet<Options> options = EnumSet.of(Options.INDEX_ON_THE_FLY);
		final VariantContextWriter out = new VariantContextWriterBuilder().setReferenceDictionary(d).setOptions(options)
				.modifyOption(Options.ALLOW_MISSING_FIELDS_IN_HEADER, true).setOutputFile(output).build();
		out.writeHeader(readerVcf.getFileHeader());
		readerVcf.close();

		for (final VariantContext variantContext : sorter) {
			out.add(variantContext);
		}
		out.close();
		sorter.cleanup();
		FileUtil.deleteFile(outputUnsorted);

		return errors;
	}

	public static void main(String[] args) throws IOException {
		// String input =
		// "/home/lukas/cloud/Genepi/Testdata/imputationserver/chr20.R50.merged.1.330k.recode.vcf.gz";
		// String output =
		// "/home/lukas/cloud/Genepi/Testdata/imputationserver/chr20.R50.merged.1.330k.recode.hg38.vcf.gz";

		String input = "/home/lukas/git/imputationserver-public2/test-data/data/big/chr1-wrayner-filtered-reheader.vcf.gz";
		String output = "lf.hg38.vcf.gz";

		String chainFile = "files/minimac/hg19ToHg38.over.chain.gz";
		long start = System.currentTimeMillis();
		List<String> errors = VcfLiftOver.liftOver(input, output, chainFile, "./temp");
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
