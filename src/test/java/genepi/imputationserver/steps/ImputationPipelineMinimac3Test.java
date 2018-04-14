package genepi.imputationserver.steps;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.groovy.control.CompilationFailedException;

import groovy.text.SimpleTemplateEngine;
import junit.framework.TestCase;

public class ImputationPipelineMinimac3Test extends TestCase {

	public static final boolean VERBOSE = true;

	public void testWithWrongReferencePanel() throws IOException, CompilationFailedException, ClassNotFoundException {

		String template = "--refHaps ${ref} --haps ${vcf} --start ${start} --end ${end} --window ${window} --prefix ${prefix} --chr ${chr} --noPhoneHome --format GT,DS,GP --allTypedSites --meta --minRatio 0.00001 ${unphased ? '--unphasedOutput' : ''} ${mapMinimac != null ? '--referenceEstimates --map ' + mapMinimac : ''}";

		Map<String, Object> binding = new HashMap<String, Object>();
		binding.put("ref", "ref.txt");
		binding.put("vcf", "vcf.txt");
		binding.put("start", 55);
		binding.put("end", 100);
		binding.put("window", 22);
		binding.put("prefix", "output-prefix");
		binding.put("chr", 22);
		binding.put("unphased", true);
		binding.put("mapMinimac", null);

		SimpleTemplateEngine engine = new SimpleTemplateEngine();
		String outputTemplate = engine.createTemplate(template).make(binding).toString();

		assertEquals(
				"--refHaps ref.txt --haps vcf.txt --start 55 --end 100 --window 22 --prefix output-prefix --chr 22 --noPhoneHome --format GT,DS,GP --allTypedSites --meta --minRatio 0.00001 --unphasedOutput ",
				outputTemplate);

	}

	public void testWithWrongReferencePanelUnphased()
			throws IOException, CompilationFailedException, ClassNotFoundException {

		String template = "--refHaps ${ref} --haps ${vcf} --start ${start} --end ${end} --window ${window} --prefix ${prefix} --chr ${chr} --noPhoneHome --format GT,DS,GP --allTypedSites --meta --minRatio 0.00001 ${unphased ? '--unphasedOutput' : ''} ${mapMinimac != null ? '--referenceEstimates --map ' + mapMinimac : ''}";

		Map<String, Object> binding = new HashMap<String, Object>();
		binding.put("ref", "ref.txt");
		binding.put("vcf", "vcf.txt");
		binding.put("start", 55);
		binding.put("end", 100);
		binding.put("window", 22);
		binding.put("prefix", "output-prefix");
		binding.put("chr", 22);
		binding.put("unphased", false);
		binding.put("mapMinimac", null);

		SimpleTemplateEngine engine = new SimpleTemplateEngine();
		String outputTemplate = engine.createTemplate(template).make(binding).toString();

		assertEquals(
				"--refHaps ref.txt --haps vcf.txt --start 55 --end 100 --window 22 --prefix output-prefix --chr 22 --noPhoneHome --format GT,DS,GP --allTypedSites --meta --minRatio 0.00001  ",
				outputTemplate);

	}

	public void testWithWrongReferencePanelMapMinimac()
			throws IOException, CompilationFailedException, ClassNotFoundException {

		String template = "--refHaps ${ref} --haps ${vcf} --start ${start} --end ${end} --window ${window} --prefix ${prefix} --chr ${chr} --noPhoneHome --format GT,DS,GP --allTypedSites --meta --minRatio 0.00001 ${unphased ? '--unphasedOutput' : ''} ${mapMinimac != null ? '--referenceEstimates --map ' + mapMinimac : ''}";

		Map<String, Object> binding = new HashMap<String, Object>();
		binding.put("ref", "ref.txt");
		binding.put("vcf", "vcf.txt");
		binding.put("start", 55);
		binding.put("end", 100);
		binding.put("window", 22);
		binding.put("prefix", "output-prefix");
		binding.put("chr", 22);
		binding.put("unphased", false);
		binding.put("mapMinimac", "lukas");

		SimpleTemplateEngine engine = new SimpleTemplateEngine();
		String outputTemplate = engine.createTemplate(template).make(binding).toString();

		assertEquals(
				"--refHaps ref.txt --haps vcf.txt --start 55 --end 100 --window 22 --prefix output-prefix --chr 22 --noPhoneHome --format GT,DS,GP --allTypedSites --meta --minRatio 0.00001  --referenceEstimates --map lukas",
				outputTemplate);

	}

}
