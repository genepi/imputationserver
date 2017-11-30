package genepi.imputationserver;

import java.lang.reflect.InvocationTargetException;

import genepi.base.Toolbox;
import genepi.imputationserver.tools.LegendFileLiftOverTool;
import genepi.imputationserver.tools.LegendFileTool;
import genepi.imputationserver.tools.VcfLiftOverTool;

public class Main extends Toolbox {

	public Main(String command, String[] args) {
		super(command, args);
	}

	public static void main(String[] args) throws InstantiationException, IllegalAccessException, SecurityException,
			NoSuchMethodException, IllegalArgumentException, InvocationTargetException {

		Main main = new Main("imputationserver.jar", args);
		main.addTool("legend", LegendFileTool.class);
		main.addTool("legend-liftover", LegendFileLiftOverTool.class);
		main.addTool("vcf-liftover", VcfLiftOverTool.class);

		main.start();
	}
}
