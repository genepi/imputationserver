package genepi.imputationserver.steps.fastqc;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Vector;

import genepi.io.text.LineReader;
import htsjdk.variant.vcf.VCFFileReader;
import htsjdk.variant.vcf.VCFHeader;

public class FastVCFFileReader extends LineReader {

	private List<String> samples;

	private int snpsCount = 0;

	private int samplesCount = 0;

	private int samplesInLineCount = 0;

	private int noCallCount = 0;

	private int hetCount = 0;

	private int homRefCount = 0;

	private int homVarCount = 0;

	private int i = 0;

	private int countR = 0;

	private int countV = 0;

	private int countNo = 0;
		
	private MinimalVariantContext variantContext;

	private List<String> header = new Vector<>();

	public FastVCFFileReader(String vcfFilename) throws IOException {

		super(vcfFilename);
		// load header
		VCFFileReader reader = new VCFFileReader(new File(vcfFilename), false);
		VCFHeader header = reader.getFileHeader();
		samples = header.getGenotypeSamples();
		samplesCount = samples.size();
		variantContext = new MinimalVariantContext(samplesCount);
		reader.close();

	}

	public List<String> getGenotypedSamples() {
		return samples;
	}

	public MinimalVariantContext getVariantContext() {
		return variantContext;
	}

	public int getSnpsCount() {
		return snpsCount;
	}

	public int getSamplesCount() {
		return samplesCount;
	}

	@Override
	protected void parseLine(String line) throws IOException {

		// not a header line
		if (line.charAt(0) != '#') {

			String tiles[] = line.split("\t", 10);

			if (tiles.length < 3) {
				throw new IOException("The provided VCF file is not tab-delimited");
			}

			String chromosome = tiles[0];
			int position = Integer.parseInt(tiles[1]);
			String ref = tiles[3];
			String alt = tiles[4];

			i = 0;
			countR = 0;
			countV = 0;

			homRefCount = 0;
			homVarCount = 0;
			hetCount = 0;
			noCallCount = 0;
			
			samplesInLineCount = 0;
			while (i < tiles[9].length()) {
				countR = 0;
				countV = 0;
				countNo = 0;
				// count genotpyes for one sample
				while (i < tiles[9].length() && tiles[9].charAt(i) != '\t') {
					if (tiles[9].charAt(i) == '1') {
						countV++;
					} else if (tiles[9].charAt(i) == '0') {
						countR++;
					} else if (tiles[9].charAt(i) == '.') {
						countNo++;
					}
					i++;
				}
				// check if it is hom or het
				if (countR == 2 || (countR == 1 && countV == 0)) {
					homRefCount++;
				} else if (countV == 2 || (countV == 1 && countR == 0)) {
					homVarCount++;
				} else

				if (countV == 1 && countR == 1) {
					hetCount++;
				}
				i++;
				
				if (countNo==2 || (countNo==1 &&countV == 0 && countR == 0)){
					noCallCount++;
					variantContext.setCalled(samplesInLineCount, false);
				}else{
					variantContext.setCalled(samplesInLineCount, true);
				}
							
				samplesInLineCount++;
			}

			if (samplesInLineCount != samplesCount) {
				throw new IOException("Line " + getLineNumber() + ": different number of samples.");
			}

			// update variant context
			variantContext.setContig(chromosome);
			variantContext.setStart(position);
			variantContext.setReferenceAllele(ref);
			variantContext.setAlternateAllele(alt);
			variantContext.setHetCount(hetCount);
			variantContext.setHomRefCount(homRefCount);
			variantContext.setHomVarCount(homVarCount);
			variantContext.setNoCallCount(noCallCount);
			variantContext.setNSamples(samplesInLineCount);
			variantContext.setRawLine(line);
			if (!tiles[6].equals("PASS")){
				variantContext.setFilters(tiles[6]);
			}else{
				variantContext.setFilters(null);
			}
			
			snpsCount++;
		}else{
			header.add(line);
			next();
		}

	}

	public List<String> getFileHeader() {
		return header;
	}

}
