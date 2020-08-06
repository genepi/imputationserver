package genepi.imputationserver.steps.vcf;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import genepi.imputationserver.steps.vcf.sort.VcfLine;
import genepi.imputationserver.steps.vcf.sort.VcfLineSortingCollection;
import genepi.io.text.LineReader;
import htsjdk.samtools.liftover.LiftOver;
import htsjdk.samtools.util.Interval;
import htsjdk.samtools.util.SortingCollection;

public class VcfLiftOverFast {

	private static final int MAX_RECORDS_IN_RAM = 1000;

	public static final Map<String, String> ALLELE_SWITCHES = new HashMap<String, String>();

	static {
		ALLELE_SWITCHES.put("A", "T");
		ALLELE_SWITCHES.put("T", "A");
		ALLELE_SWITCHES.put("G", "C");
		ALLELE_SWITCHES.put("C", "G");
	}

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
				String newContig = "";

				if (vcfLine.getContig().equals("chr23")) {
					vcfLine.setContig("chrX");
				}
				if (vcfLine.getContig().equals("23")) {
					vcfLine.setContig("X");
				}

				if (vcfLine.getContig().startsWith("chr")) {
					contig = vcfLine.getContig();
					newContig = vcfLine.getContig().replaceAll("chr", "");

				} else {
					contig = "chr" + vcfLine.getContig();
					newContig = "chr" + vcfLine.getContig();
				}

				int length = vcfLine.getReference().length();
				int start = vcfLine.getPosition();
				int stop = vcfLine.getPosition() + length - 1;

				Interval source = new Interval(contig, start, stop, false, vcfLine.getId());

				Interval target = liftOver.liftOver(source);

				if (target != null) {

					if (source.getContig().equals(target.getContig())) {

						if (length != target.length()) {

							errors.add(vcfLine.getContig() + ":" + vcfLine.getPosition() + "\t" + "LiftOver" + "\t"
									+ "INDEL_STRADDLES_TWO_INTERVALS. SNP removed.");

						} else {

							if (target.isNegativeStrand()) {

								vcfLine.setReference(switchAllel(vcfLine.getReference()));
								vcfLine.setAlternate(switchAllel(vcfLine.getAlternate()));

							}

							if (vcfLine.getReference() != null && vcfLine.getAlternate() != null) {

								vcfLine.setContig(newContig);
								vcfLine.setPosition(target.getStart());
								sorter.add(vcfLine);

							} else {

								errors.add(vcfLine.getContig() + ":" + vcfLine.getPosition() + "\t" + "LiftOver" + "\t"
										+ "Indel on negative strand. SNP removed.");

							}
						}

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

		int pos = -1;
		for (VcfLine vcfLine : sorter) {
			if (vcfLine.getPosition() < pos) {
				throw new IOException("Sorting VCF file after Liftover failed.");
			}
			writer.write(vcfLine.getLine());
			pos = vcfLine.getPosition();
		}
		writer.close();
		sorter.cleanup();

		return errors;
	}

	public static String switchAllel(String allele) {
		return ALLELE_SWITCHES.get(allele);
	}

}
