package genepi.imputationserver.tools;

import genepi.base.Tool;
import genepi.imputationserver.steps.imputation.ImputationPipeline;

public class VersionTool extends Tool {

	public VersionTool(String[] args) {
		super(args);
	}

	@Override
	public void createParameters() {
	
	}

	@Override
	public void init() {
		
		System.out.println("Michigan Imputation Server");
		
	}

	@Override
	public int run() {

		System.out.println();
		System.out.println("Pipeline: " + ImputationPipeline.PIPELINE_VERSION);
		System.out.println("Imputation-Engine: " + ImputationPipeline.IMPUTATION_VERSION);
		System.out.println("Phasing-Engine: " + ImputationPipeline.PHASING_VERSION);
		System.out.println();
		
		return 0;

	}

}
