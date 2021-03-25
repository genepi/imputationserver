package genepi.imputationserver.util;

import genepi.imputationserver.steps.imputation.ImputationPipeline;

public class ImputationParameters {

	private String referencePanelName;

	private double minR2;

	private String phasing;

	private boolean phasingRequired;

	public String getReferencePanelName() {
		return referencePanelName;
	}

	public void setReferencePanelName(String referencePanelName) {
		this.referencePanelName = referencePanelName;
	}

	public double getMinR2() {
		return minR2;
	}

	public void setMinR2(double minR2) {
		this.minR2 = minR2;
	}

	public String getPhasing() {
		return phasing;
	}

	public void setPhasing(String phasing) {
		this.phasing = phasing;
	}

	public void setPhasingRequired(boolean phasingRequired) {
		this.phasingRequired = phasingRequired;
	}

	public String getPhasingMethod() {
		
		if (phasingRequired) {
			if (phasing.equals("eagle")) {
				return ImputationPipeline.EAGLE_VERSION;
			} else if (phasing.equals("beagle")) {
				return ImputationPipeline.BEAGLE_VERSION;
			}
		} else {
			return "n/a";
		}
		return "";

	}

}
