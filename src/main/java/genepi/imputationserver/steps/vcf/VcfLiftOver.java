package genepi.imputationserver.steps.vcf;

import java.io.File;
import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Vector;

import genepi.hadoop.command.Command;
import genepi.io.FileUtil;
import genepi.io.text.LineReader;
import htsjdk.samtools.ValidationStringency;
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

	private static final int MAX_RECORDS_IN_RAM = 500000;
	public static String BCFTOOLS_PATH = "bin/";

	public static void setBinary(String binaries) {
		BCFTOOLS_PATH = binaries;
	}

	public static String getBinary() {
		return BCFTOOLS_PATH;
	}

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
						writer.write(tiles[0] + "\t" + target.getStart() + "\t" + tiles[2]);
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
			contigs.add(i + "");
		}

		// read from file and add to sorter
		final SortingCollection<VariantContext> sorter = SortingCollection.newInstance(VariantContext.class,
				new VCFRecordCodec(readerVcf.getFileHeader(), true), new VariantContextComparator(contigs),
				MAX_RECORDS_IN_RAM, new File(tempDir));
		for (final VariantContext variantContext : readerVcf) {
			sorter.add(variantContext);
		}

		// write from sorter to file
		final EnumSet<Options> options = EnumSet.of(Options.INDEX_ON_THE_FLY);
		final VariantContextWriter out = new VariantContextWriterBuilder()
				.setReferenceDictionary(readerVcf.getFileHeader().getSequenceDictionary()).setOptions(options)
				.setOutputFile(output).build();
		out.writeHeader(readerVcf.getFileHeader());
		for (final VariantContext variantContext : sorter) {
			out.add(variantContext);
		}
		out.close();
		readerVcf.close();
		FileUtil.deleteFile(outputUnsorted);

		return errors;
	}

	public static void main(String[] args) throws IOException {
		String input = "/Users/lukas/git/imputationserver-genepi/test-data/data/chr20-phased/chr20.R50.merged.1.330k.recode.small.vcf.gz";
		String output = "/Users/lukas/git/imputationserver-genepi/test-data/data/chr20-phased/chr20.R50.merged.1.330k.recode.small.hg38.vcf.gz";
		String chainFile = "files/minimac/hg19ToHg38.over.chain.gz";
		VcfLiftOver.liftOver(input, output, chainFile, "");

		VcfFileUtil.load(output, 1000000, false);
	}

}
