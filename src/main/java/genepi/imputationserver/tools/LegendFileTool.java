package genepi.imputationserver.tools;

import genepi.base.Tool;
import genepi.imputationserver.steps.fastqc.legend.LegendFileReader;
import genepi.io.text.GzipLineWriter;
import genepi.io.text.LineReader;
import genepi.io.text.LineWriter;

public class LegendFileTool extends Tool {

	public LegendFileTool(String[] args) {
		super(args);
	}

	@Override
	public void createParameters() {
		addParameter("vcf", "input vcf file");
		addParameter("output", "output legend file");
		addParameter("reference", "reference legend file", Tool.STRING);
		addOptionalParameter("limit", "max records", Tool.INTEGER);
	}

	@Override
	public void init() {
		System.out.println("LegendFile Generator for Michigan Imputation Server");
		System.out.println("");
	}

	@Override
	public int run() {

		String input = getValue("vcf").toString();
		String output = getValue("output").toString();
		String reference = getValue("reference").toString();
		Integer limit = Integer.MAX_VALUE;
		if (getValue("limit") != null) {
			limit = (Integer) getValue("limit");
		}

		try {

			System.out.println("Load header from legend file " + reference + "...");
			LineReader legendFileHeaderReader = new LineReader(reference);
			legendFileHeaderReader.next();
			String header = legendFileHeaderReader.get();
			legendFileHeaderReader.close();

			if (header == null) {
				System.out.println("Error: header is null.");
				return 1;
			}

			System.out.println("Load reference legend file " + reference + "...");
			LegendFileReader legendFileReader = new LegendFileReader(reference, "");

			System.out.println("  Create index...");
			legendFileReader.createIndex();

			System.out.println("  Init search...");
			legendFileReader.initSearch();

			System.out.println("Load input vcf file " + input + "...");
			LineReader reader = new LineReader(input);

			System.out.println("Open output file " + output + "...");
			GzipLineWriter writer = new GzipLineWriter(output);
			writer.write(header);

			String[] headerTiles = header.split(" ");

			int found = 0;
			int notFound = 0;

			int count = 0;
			while (reader.next() && count < limit) {
				String line = reader.get();
				if (!line.startsWith("#")) {
					String[] tiles = line.split("\t", 6);
					String id = tiles[2];
					String position = tiles[1];
					String referenceLine = legendFileReader.findLineByPosition(Integer.parseInt(position));

					String newLine = id + " " + position + " " + tiles[3] + " " + tiles[4];
					if (referenceLine != null) {
						// use frequencies from reference file
						found++;
						String[] newTiles = referenceLine.split(" ");
						for (int i = 4; i < newTiles.length; i++) {
							newLine += " " + newTiles[i];
						}
					} else {
						// set frequencies to missing
						notFound++;
						for (int i = 4; i < headerTiles.length; i++) {
							newLine += " " + ".";
						}
					}

					count++;
					writer.write(newLine);
				}
			}

			legendFileReader.close();
			reader.close();
			writer.close();

			LineWriter log = new LineWriter(output + ".log");
			log.write("Input VCF file: " + input);
			log.write("Reference gegend file: " + reference);
			log.write("  Written " + count + " records.");
			log.write("  Records with frequency: " + found + " (" + 100 * found / (double) count + "%)");
			log.write("  Records with missings: " + notFound + " (" + 100 * notFound / (double) count + "%)");
			log.close();

			System.out.println("Input VCF file: " + input);
			System.out.println("Reference gegend file: " + reference);
			System.out.println("  Written " + count + " records.");
			System.out.println("  Records with frequency: " + found + " (" + 100 * found / (double) count + "%)");
			System.out.println("  Records with missings: " + notFound + " (" + 100 * notFound / (double) count + "%)");

			return 0;

		} catch (Exception e) {
			e.printStackTrace();
			return 1;

		}

	}

}
