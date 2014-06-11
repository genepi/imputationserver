package genepi.minicloudmac.hadoop.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class ParameterStore {

	private Context context;

	private Configuration configuration;

	public ParameterStore(Context context) {
		this.configuration = context.getConfiguration();
	}

	public String get(String parameter) {
		return configuration.get(parameter);
	}

	public String get(String parameter, String defaultValue) {
		return configuration.get(parameter, defaultValue);
	}

	public int getInteger(String parameter) {
		return configuration.getInt(parameter, 0);
	}

	public int getInteger(String parameter, int defaultValue) {
		return configuration.getInt(parameter, defaultValue);
	}

}
