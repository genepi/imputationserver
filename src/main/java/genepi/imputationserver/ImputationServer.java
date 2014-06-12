package genepi.imputationserver;

import genepi.base.Toolbox;
import genepi.imputationserver.steps.imputation.Imputation;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

public class ImputationServer {

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
		toolbox.addTool("imputation", Imputation.class);
		toolbox.addTool("quality-control",
				genepi.imputationserver.steps.QualityControl.class);
		toolbox.start();

	}

}
