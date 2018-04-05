package genepi.imputationserver.util;

import java.util.Map;
import java.util.HashMap;
import genepi.hadoop.PreferenceStore;

public class DefaultPreferenceStore {

	public static void init(PreferenceStore store) {

		Map<String, String> defaults = new HashMap<String, String>();
		defaults.put("chunksize", "20000000");
		defaults.put("phasing.window", "5000000");
		defaults.put("minimac.window", "500000");
		defaults.put("minimac.rounds", "0");
		defaults.put("samples.max", "0");
		defaults.put("minimac.sendmail", "no");
		defaults.put("server.url", "https://imputationserver.sph.umich.edu");
		defaults.put("minimac.tmp", "/tmp");
		defaults.put("minimac.command",
				"--refHaps ${ref} --haps ${vcf} --start ${start} --end ${end} --window ${window} --prefix ${prefix} --chr ${chr} --noPhoneHome --format GT,DS,GP --allTypedSites --meta --minRatio 0.00001 ${unphased ? '--unphasedOutput' : ''} ${mapMinimac != null ? '--referenceEstimates --map ' + mapMinimac : ''}");
		defaults.put("ref.fasta", "human_g1k_v37.fasta");
		defaults.put("hg38Tohg19", "chains/hg38ToHg19.over.chain.gz");
		defaults.put("hg19Tohg38", "chains/hg19ToHg38.over.chain.gz");

		//set all empty values to default values
		
		for (String key: defaults.keySet()){
			String value = store.getString(key);
			if (value == null){
				value = defaults.get(key);
				store.setString(key, value);
			}
		}
		
	}

}
