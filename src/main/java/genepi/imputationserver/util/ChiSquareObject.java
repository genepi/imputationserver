package genepi.imputationserver.util;

public class ChiSquareObject {

	private double p;
	private double q;
	private double chisq;
	
	public ChiSquareObject(double chisq, double p, double q){
		this.chisq = chisq;
		this.p = p;
		this.q = q;
		
	}
	public double getP() {
		return p;
	}
	public void setP(double p) {
		this.p = p;
	}
	public double getQ() {
		return q;
	}
	public void setQ(double q) {
		this.q = q;
	}
	public double getChisq() {
		return chisq;
	}
	public void setChisq(double chisq) {
		this.chisq = chisq;
	}
}
