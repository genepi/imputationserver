package genepi.minicloudmac;

import genepi.base.Toolbox;
import genepi.minicloudmac.hadoop.imputation.ImputationVcf;
import genepi.minicloudmac.hadoop.install.Install;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

public class MiniCloudMac {

	/**
	 * @param args
	 * @throws IOException
	 * @throws InvocationTargetException
	 * @throws IllegalArgumentException
	 * @throws NoSuchMethodException
	 * @throws SecurityException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	public static void main(String[] args) throws IOException,
			InstantiationException, IllegalAccessException, SecurityException,
			NoSuchMethodException, IllegalArgumentException,
			InvocationTargetException {

		Toolbox toolbox = new Toolbox("minimac-cloud.jar", args);
		toolbox.addTool("install", Install.class);
		toolbox.addTool("minimac-vcf", ImputationVcf.class);
		toolbox.addTool("maf-vcf",
				genepi.minicloudmac.hadoop.preprocessing.vcf.Maf.class);
		toolbox.start();

	}

}
