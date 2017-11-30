package genepi.imputationserver.steps.vcf;

import java.io.IOException;

public class VCFLineParser {

	private int samplesInLineCount = 0;

	private int noCallCount = 0;

	private int hetCount = 0;

	private int homRefCount = 0;

	private int homVarCount = 0;

	private int i = 0;

	private int countR = 0;

	private int countV = 0;

	private int countNo = 0;

	private int tile = 0;

	private int tileGT = 0;
	
	private int j = 0;
	
	private int k = 0;

	private MinimalVariantContext variantContext;

	public VCFLineParser(int samples) {
		variantContext = new MinimalVariantContext(samples);
	}

	public MinimalVariantContext parseLine(String line) throws IOException {

		String tiles[] = line.split("\t", 10);

		if (tiles.length < 10) {
			throw new IOException("The provided VCF file is not correct tab-delimited");
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

		tileGT = 0;
		k = tiles[8].indexOf("GT");
		
		if (k == -1){
			throw new IOException("No GT field found in FORMAT column.");
		}
		
		j = 0;
		while (j < k){
			if (tiles[8].charAt(j) == ':'){
				tileGT++;
			}
			j++;
		}
				
		while (i < tiles[9].length()) {
			countR = 0;
			countV = 0;
			countNo = 0;
			// count genotypes for one sample
			tile = 0;
			while (i < tiles[9].length() && tiles[9].charAt(i) != '\t') {

				//count format values 
				if (tiles[9].charAt(i) == ':') {
					tile++;
				} else {
					//find right position
					if (tile == tileGT) {

						if (tiles[9].charAt(i) == '1') {
							countV++;
						} else if (tiles[9].charAt(i) == '0') {
							countR++;
						} else if (tiles[9].charAt(i) == '.') {
							countNo++;
						}
					}
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

			if (countNo == 2 || (countNo == 1 && countV == 0 && countR == 0)) {
				noCallCount++;
				variantContext.setCalled(samplesInLineCount, false);
			} else {
				variantContext.setCalled(samplesInLineCount, true);
			}

			samplesInLineCount++;
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

		if (!tiles[6].equals("PASS") && !tiles[6].equals(".")) {
			variantContext.setFilters(tiles[6]);
		} else {
			variantContext.setFilters(null);
		}

		return variantContext;
	}

}
