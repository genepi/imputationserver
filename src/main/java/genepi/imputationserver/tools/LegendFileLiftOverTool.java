package genepi.imputationserver.tools;

import genepi.base.Tool;
import genepi.imputationserver.util.LegendFileLiftOver;
import genepi.imputationserver.util.LegendFileLiftOver.LegendFileLiftOverResults;
import genepi.io.text.LineWriter;

public class LegendFileLiftOverTool extends Tool {

	public LegendFileLiftOverTool(String[] args) {
		super(args);
	}

	@Override
	public void createParameters() {
		addParameter("input", "input vcf file");
		addParameter("output", "output vcf file");
		addParameter("chr", "chromosome");
		addParameter("chain", "chain file");
	}

	@Override
	public void init() {
		System.out.println("LegendFile LiftOver Tool");
		System.out.println("");
	}

	@Override
	public int run() {

		String input = getValue("input").toString();
		String output = getValue("output").toString();
		String chain = getValue("chain").toString();
		String chromosome = getValue("chr").toString();

		try {
			LegendFileLiftOverResults results = LegendFileLiftOver.liftOver(input, output, chain, "", chromosome);
			System.out.println("Snps written: " + results.snpsWritten);
			System.out.println("Snps lost: " + results.errors.size());

			LineWriter log = new LineWriter(output + ".log");
			log.write("Input VCF file: " + input);
			log.write("Chain file: " + chain);
			log.write("Chromosome: " + chromosome);
			log.write("  Snps written: " + results.snpsWritten);
			log.write("  Snps lost: " + results.errors.size());
			log.write("");
			for (String error : results.errors) {
				log.write("  " + error);
			}
			log.close();

			return 0;
		} catch (Exception e) {
			e.printStackTrace();
			return 1;
		}

	}

}
