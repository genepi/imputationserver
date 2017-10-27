package genepi.imputationserver.tools;

import genepi.base.Tool;
import genepi.imputationserver.steps.vcf.VcfLiftOverFast;

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
			VcfLiftOverFast.liftOver(input, output, chain, "./");

			return 0;
		} catch (Exception e) {
			e.printStackTrace();
			return 1;
		}

	}

}
