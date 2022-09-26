package genepi.imputationserver.steps.ancestry;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import genepi.io.table.reader.CsvTableReader;
import genepi.io.table.writer.CsvTableWriter;

public class PopulationPredictor {

	private static final String LABEL_UNKNOWN = "unknown";

	private String studyFile = null;

	private String referenceFile = null;

	private String samplesFile = null;

	private int maxPcs = 3;

	private int K = 10;

	private double WEIGHT_THRESHOLD = 0.75; //0.875

	public PopulationPredictor() {

	}

	public void predictPopulation(String output) {

		Map<String, ReferenceSample> samplesIndex = new HashMap<String, ReferenceSample>();
		CsvTableReader samplesReader = new CsvTableReader(samplesFile, '\t');
		while (samplesReader.next()) {
			String id = samplesReader.getString("indivID");
			String label = samplesReader.getString("superpopID");
			ReferenceSample sample = new ReferenceSample();
			sample.setLabel(label);
			samplesIndex.put(id, sample);
		}
		samplesReader.close();
		System.out.println("Loaded " + samplesIndex.size() + " reference samples.");

		List<ReferenceSample> samples = new Vector<ReferenceSample>();
		samplesReader = new CsvTableReader(referenceFile, '\t');
		while (samplesReader.next()) {
			String id = samplesReader.getString("indivID");
			double[] pcs = new double[maxPcs];
			for (int i = 0; i < pcs.length; i++) {
				pcs[i] = samplesReader.getDouble("PC" + (i + 1));
			}
			ReferenceSample sample = samplesIndex.get(id);
			sample.setPcs(pcs);
			samples.add(sample);
		}
		samplesReader.close();
		System.out.println(
				"Loaded " + maxPcs + " PCs for " + samples.size() + "/" + samplesIndex.size() + " reference samples.");

		CsvTableWriter writer = new CsvTableWriter(output, '\t');
		String[] columns = new String[5 + maxPcs];
		columns[0] = "sample";
		columns[1] = "population";
		columns[2] = "voting_popluation";
		columns[3] = "voting";
		for (int i = 0; i < maxPcs; i++) {
			columns[i + 4] = "PC" + (i+1);
		}
		writer.setColumns(columns);

		int samplesCount = 0;
		CsvTableReader studyReader = new CsvTableReader(studyFile, '\t');
		while (studyReader.next()) {
			String id = studyReader.getString("indivID");
			double[] pcs = new double[maxPcs];
			for (int i = 0; i < pcs.length; i++) {
				pcs[i] = studyReader.getDouble("PC" + (i + 1));
			}
			Neighbor[] neighbors = getNearestNeighbors(samples, pcs, K);
			PredictedPopulation[] voting = getVoting(neighbors);
			writer.setString("sample", id);
			if (voting[0].getWeight() >= WEIGHT_THRESHOLD) {
				writer.setString("population", voting[0].getLabel());
			} else {
				writer.setString("population", LABEL_UNKNOWN);
			}
			writer.setString("voting_popluation", voting[0].getLabel());
			writer.setDouble("voting", voting[0].getWeight());
			for (int i = 0; i < pcs.length; i++) {
				writer.setDouble("PC" + (i+1), pcs[i]);
			}
			writer.next();
			samplesCount++;
		}
		studyReader.close();
		writer.close();
		System.out.println("Predicted the population for " + samplesCount + " samples");

	}

	public void setReferenceFile(String referenceFile) {
		this.referenceFile = referenceFile;
	}

	public void setSamplesFile(String samplesFile) {
		this.samplesFile = samplesFile;
	}

	public void setStudyFile(String studyFile) {
		this.studyFile = studyFile;
	}
	
	public void setMaxPcs(int maxPcs) {
		this.maxPcs = maxPcs;
	}

	protected PredictedPopulation[] getVoting(Neighbor[] neighbors) {
		Map<String, PredictedPopulation> populations = new HashMap<String, PredictedPopulation>();
		for (int i = 0; i < neighbors.length; i++) {
			Neighbor neighbor = neighbors[i];
			String label = neighbor.getSample().getLabel();
			PredictedPopulation population = populations.get(label);
			if (population == null) {
				population = new PredictedPopulation();
				population.setLabel(label);
				populations.put(label, population);
			}
			population.addSample(1.0 / neighbor.getDistance());
		}

		PredictedPopulation[] result = new PredictedPopulation[populations.size()];
		int c = 0;
		double sum = 0;
		for (PredictedPopulation population : populations.values()) {
			result[c] = population;
			sum += population.getWeight();
			c++;
		}
		for (PredictedPopulation population : populations.values()) {
			population.setSumWeight(population.getWeight() / sum);
		}
		Arrays.sort(result);

		return result;
	}

	protected Neighbor[] getNearestNeighbors(Collection<ReferenceSample> samples, double[] pcs, int k) {

		Neighbor[] neighbors = new Neighbor[samples.size()];
		int c = 0;
		for (ReferenceSample sample : samples) {
			double distance = sample.distanceTo(pcs);
			Neighbor neighbor = new Neighbor(sample, distance);
			neighbors[c] = neighbor;
			c++;
		}

		Arrays.sort(neighbors);

		Neighbor[] topNeighbors = new Neighbor[k];
		for (int i = 0; i < k; i++) {
			topNeighbors[i] = neighbors[i];
		}
		return topNeighbors;
	}

	class Neighbor implements Comparable<Neighbor> {

		private ReferenceSample sample;

		private double distance;

		public Neighbor(ReferenceSample sample, double distance) {
			super();
			this.sample = sample;
			this.distance = distance;
		}

		public double getDistance() {
			return distance;
		}

		public ReferenceSample getSample() {
			return sample;
		}

		@Override
		public int compareTo(Neighbor o) {
			return Double.compare(distance, o.getDistance());
		}
	}

	class ReferenceSample {

		private String label;

		private double[] pcs;

		public void setLabel(String label) {
			this.label = label;
		}

		public String getLabel() {
			return label;
		}

		public void setPcs(double[] pcs) {
			this.pcs = pcs;
		}

		public double[] getPcs() {
			return pcs;
		}

		public double distanceTo(double[] point) {
			double result = 0;
			for (int i = 0; i < pcs.length; i++) {
				result += Math.pow(point[i] - pcs[i], 2);
			}
			return Math.sqrt(result);
		}
	}

	class PredictedPopulation implements Comparable<PredictedPopulation> {

		private String label;

		private double sumWeight = 0;

		private int count = 0;

		public void setLabel(String label) {
			this.label = label;
		}

		public String getLabel() {
			return label;
		}

		public double getWeight() {
			return sumWeight;// / (double) count;
		}

		public void setSumWeight(double sumWeight) {
			this.sumWeight = sumWeight;
		}

		public void addSample(double weight) {
			count++;
			this.sumWeight += weight;
		}

		public int getCount() {
			return count;
		}

		@Override
		public int compareTo(PredictedPopulation o) {
			return -Double.compare(getWeight(), o.getWeight());
		}

	}

	public static void main(String[] args) {
		PopulationPredictor predictor = new PopulationPredictor();
		predictor.setSamplesFile("/Users/lukfor/Data/laser/apps/trace/references/HGDP_238_chr22.samples");
		predictor.setReferenceFile(
				"/Users/lukfor/Data/projects/humangen/archive/20201215_humangen_part1/output/02_imputation/reference_pc.txt");
		predictor.setStudyFile(
				"/Users/lukfor/Data/projects/humangen/archive/20201215_humangen_part1/output/02_imputation/study_pc.txt");
		predictor.predictPopulation("test.txt");
	}

}
