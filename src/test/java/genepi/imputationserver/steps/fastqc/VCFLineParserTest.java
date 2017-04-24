package genepi.imputationserver.steps.fastqc;

import java.io.IOException;

import junit.framework.TestCase;

public class VCFLineParserTest extends TestCase {

	public void testNormalLine() throws IOException {

		int samples = 3;
		String line = "20	14370	rs6054257	G	A	29	PASS	NS=3;DP=14;AF=0.5;DB;H2	GT	0/0	1/0	1/1";

		VCFLineParser parser = new VCFLineParser(samples);
		MinimalVariantContext variantContext = parser.parseLine(line);

		assertEquals(samples, variantContext.getNSamples());
		assertEquals(1, variantContext.getHetCount());
		assertEquals(1, variantContext.getHomRefCount());
		assertEquals(1, variantContext.getHomVarCount());
		assertEquals("20", variantContext.getContig());
		assertEquals(14370, variantContext.getStart());
		assertEquals("G", variantContext.getReferenceAllele());
		assertEquals("A", variantContext.getAlternateAllele());		
		assertEquals(false, variantContext.isFiltered());
		assertEquals(false, variantContext.isMonomorphicInSamples());
		assertEquals(false, variantContext.isIndel());

	}

	public void testWithComplexFormat() throws IOException {

		int samples = 3;
		String line = "20	14370	rs6054257	G	A	29	PASS	NS=3;DP=14;AF=0.5;DB;H2	GT:GQ:DP:HQ	0|0:48:1:51,51	1|0:48:8:51,51	1/1:43:5:.,.";

		VCFLineParser parser = new VCFLineParser(samples);
		MinimalVariantContext variantContext = parser.parseLine(line);

		assertEquals(samples, variantContext.getNSamples());
		assertEquals(1, variantContext.getHetCount());
		assertEquals(1, variantContext.getHomRefCount());
		assertEquals(1, variantContext.getHomVarCount());
		assertEquals("20", variantContext.getContig());
		assertEquals(14370, variantContext.getStart());
		assertEquals("G", variantContext.getReferenceAllele());
		assertEquals("A", variantContext.getAlternateAllele());		
		assertEquals(false, variantContext.isFiltered());
		assertEquals(false, variantContext.isMonomorphicInSamples());
		assertEquals(false, variantContext.isIndel());

	}

	public void testWithComplexFormat2() throws IOException {

		int samples = 3;
		String line = "20	14370	rs6054257	G	A	29	.	NS=3;DP=14;AF=0.5;DB;H2	GQ:DP:GT:HQ	48:1:0|0:51,51	48:8:1|0:51,51	43:5:1/1:.,.";

		VCFLineParser parser = new VCFLineParser(samples);
		MinimalVariantContext variantContext = parser.parseLine(line);

		assertEquals(samples, variantContext.getNSamples());
		assertEquals(1, variantContext.getHetCount());
		assertEquals(1, variantContext.getHomRefCount());
		assertEquals(1, variantContext.getHomVarCount());
		assertEquals("20", variantContext.getContig());
		assertEquals(14370, variantContext.getStart());
		assertEquals("G", variantContext.getReferenceAllele());
		assertEquals("A", variantContext.getAlternateAllele());		
		assertEquals(false, variantContext.isFiltered());
		assertEquals(false, variantContext.isMonomorphicInSamples());
		assertEquals(false, variantContext.isIndel());

	}

	public void testWithFilter() throws IOException {

		int samples = 3;
		String line = "20	14370	rs6054257	G	A	29	FILTER	NS=3;DP=14;AF=0.5;DB;H2	GQ:DP:GT:HQ	48:1:0|0:51,51	48:8:1|0:51,51	43:5:1/1:.,.";

		VCFLineParser parser = new VCFLineParser(samples);
		MinimalVariantContext variantContext = parser.parseLine(line);

		assertEquals(samples, variantContext.getNSamples());
		assertEquals(1, variantContext.getHetCount());
		assertEquals(1, variantContext.getHomRefCount());
		assertEquals(1, variantContext.getHomVarCount());
		assertEquals("20", variantContext.getContig());
		assertEquals(14370, variantContext.getStart());
		assertEquals("G", variantContext.getReferenceAllele());
		assertEquals("A", variantContext.getAlternateAllele());		
		assertEquals(true, variantContext.isFiltered());
		assertEquals(false, variantContext.isMonomorphicInSamples());
		assertEquals(false, variantContext.isIndel());

	}

	public void testMonomorphic() throws IOException {

		int samples = 3;
		String line = "20	14370	rs6054257	G	A	29	.	NS=3;DP=14;AF=0.5;DB;H2	GQ:DP:GT:HQ	48:1:0|0:51,51	48:8:0|0:51,51	43:5:0/0:.,.";

		VCFLineParser parser = new VCFLineParser(samples);
		MinimalVariantContext variantContext = parser.parseLine(line);

		assertEquals(samples, variantContext.getNSamples());
		assertEquals(0, variantContext.getHetCount());
		assertEquals(3, variantContext.getHomRefCount());
		assertEquals(0, variantContext.getHomVarCount());
		assertEquals("20", variantContext.getContig());
		assertEquals(14370, variantContext.getStart());
		assertEquals("G", variantContext.getReferenceAllele());
		assertEquals("A", variantContext.getAlternateAllele());		
		assertEquals(false, variantContext.isFiltered());
		assertEquals(true, variantContext.isMonomorphicInSamples());
		assertEquals(false, variantContext.isIndel());

	}
	
	public void testMonomorphic2() throws IOException {

		int samples = 3;
		String line = "20	14370	rs6054257	G	A	29	.	NS=3;DP=14;AF=0.5;DB;H2	GQ:DP:GT:HQ	48:1:1|1:51,51	48:8:1|1:51,51	43:5:1/1:.,.";

		VCFLineParser parser = new VCFLineParser(samples);
		MinimalVariantContext variantContext = parser.parseLine(line);

		assertEquals(samples, variantContext.getNSamples());
		assertEquals(0, variantContext.getHetCount());
		assertEquals(0, variantContext.getHomRefCount());
		assertEquals(3, variantContext.getHomVarCount());
		assertEquals("20", variantContext.getContig());
		assertEquals(14370, variantContext.getStart());
		assertEquals("G", variantContext.getReferenceAllele());
		assertEquals("A", variantContext.getAlternateAllele());		
		assertEquals(false, variantContext.isFiltered());
		assertEquals(false, variantContext.isMonomorphicInSamples());
		assertEquals(false, variantContext.isIndel());
	}

	public void testWithComplexFormat3() throws IOException {

		int samples = 3;
		String line = "20	14370	rs6054257	G	A	29	PASS	NS=3;DP=14;AF=0.5;DB;H2	AA:BB:GT:CC	0|0:0|0:0|0:0|0	1|0:1|0:1|0:1|0	1|1:1|1:1|1:1|1";

		VCFLineParser parser = new VCFLineParser(samples);
		MinimalVariantContext variantContext = parser.parseLine(line);

		assertEquals(samples, variantContext.getNSamples());
		assertEquals(1, variantContext.getHetCount());
		assertEquals(1, variantContext.getHomRefCount());
		assertEquals(1, variantContext.getHomVarCount());
		assertEquals("20", variantContext.getContig());
		assertEquals(14370, variantContext.getStart());
		assertEquals("G", variantContext.getReferenceAllele());
		assertEquals("A", variantContext.getAlternateAllele());
		assertEquals(false, variantContext.isFiltered());
		assertEquals(false, variantContext.isMonomorphicInSamples());
		assertEquals(false, variantContext.isIndel());
	}

	public void testWithMissingGTFormat() {

		int samples = 3;
		String line = "20	14370	rs6054257	G	A	29	PASS	NS=3;DP=14;AF=0.5;DB;H2	GQ:DP:HQ	48:1:51,51	48:8:51,51	43:5:.,.";

		VCFLineParser parser = new VCFLineParser(samples);
		try {
			MinimalVariantContext variantContext = parser.parseLine(line);
			fail("Expected IOExepction");
		} catch (IOException e) {
			assertTrue(e.getMessage().contains("No GT field found in FORMAT column."));
		}

	}

	public void testWithWrongColumns() {

		int samples = 3;
		String line = "20	14370	rs6054257	G	A	29	PASS	NS=3;DP=14;AF=0.5;DB;H2	GQ:DP:HQ";

		VCFLineParser parser = new VCFLineParser(samples);
		try {
			MinimalVariantContext variantContext = parser.parseLine(line);
			fail("Expected IOExepction");
		} catch (IOException e) {
			assertTrue(e.getMessage().contains("The provided VCF file is not correct tab-delimited"));
		}

	}
	

	public void testIndel() throws IOException {

		int samples = 3;
		String line = "20	1234567	microsat1	GTCT	G,GTACT	50	PASS	NS=3;DP=9;AA=G	GT:GQ:DP	0/1:35:4	0/2:17:2	1/1:40:3";

		VCFLineParser parser = new VCFLineParser(samples);
	
		MinimalVariantContext variantContext = parser.parseLine(line);

		assertEquals(samples, variantContext.getNSamples());
		assertEquals("20", variantContext.getContig());
		assertEquals(1234567, variantContext.getStart());
		assertEquals("GTCT", variantContext.getReferenceAllele());
		assertEquals("G,GTACT", variantContext.getAlternateAllele());
		assertEquals(true, variantContext.isIndel());
	}
	
	//TODO: check / and | and no 0 and 1

}
