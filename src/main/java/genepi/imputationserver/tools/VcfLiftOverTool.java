package genepi.imputationserver.tools;

import java.util.List;

import genepi.base.Tool;
import genepi.imputationserver.steps.vcf.VcfLiftOverFast;
import genepi.io.text.LineWriter;

public class VcfLiftOverTool extends Tool {

	public VcfLiftOverTool(String[] args) {
		super(args);
	}

	@Override
	public void createParameters() {
		addParameter("input", "input vcf file");
		addParameter("output", "output vcf file");
		addParameter("chain", "chain file");
	}

	@Override
	public void init() {
		System.out.println("VcfLiftOver Tool");
		System.out.println("");
	}

	@Override
	public int run() {

		String input = getValue("input").toString();
		String output = getValue("output").toString();
		String chain = getValue("chain").toString();

		try {
			
			List<String> excludes = VcfLiftOverFast.liftOver(input, output, chain, "./");

			LineWriter writer = new LineWriter(output + ".excluded");
			for (String exclude: excludes) {
				writer.write(exclude);
			}
			writer.close();
			
			return 0;
			
		} catch (Exception e) {
			e.printStackTrace();
			return 1;
		}

	}

}
