package genepi.imputationserver.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import cloudgene.sdk.internal.WorkflowContext;

public class WorkflowTestContext extends WorkflowContext {

	private String jobId = "test-job-" + System.currentTimeMillis();

	private String hdfsTemp;

	private String localTemp;

	private Map<String, String> inputs;

	private Map<String, String> outputs;

	private Map<String, Integer> counters = new HashMap<String, Integer>();

	private Map<String, Boolean> submitCounters = new HashMap<String, Boolean>();

	private Map<String, Object> data = new HashMap<String, Object>();

	private Map<String, String> config = new HashMap<String, String>();

	private StringBuffer memory = new StringBuffer();

	private boolean verbose = false;

	public WorkflowTestContext(Map<String, String> inputs, Map<String, String> outputs) {
		this.inputs = inputs;
		this.outputs = outputs;
	}

	public WorkflowTestContext() {
		this.inputs = new HashMap<String, String>();
		this.outputs = new HashMap<String, String>();
	}

	@Override
	public String getInput(String param) {
		return inputs.get(param);
	}

	@Override
	public String getJobId() {
		return jobId;
	}

	@Override
	public String getOutput(String param) {
		return outputs.get(param);
	}

	@Override
	public String get(String param) {
		if (inputs.get(param) == null) {
			return outputs.get(param);
		} else {
			return inputs.get(param);
		}
	}

	@Override
	public void println(String line) {
		printAndKeep("[PRINTLN] " + line);
	}

	@Override
	public void log(String line) {
		printAndKeep("[LOG] " + line);
	}

	@Override
	public String getWorkingDirectory() {
		return "";
	}

	@Override
	public boolean sendMail(String subject, String body) throws Exception {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean sendMail(String to, String subject, String body) throws Exception {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean sendNotification(String body) throws Exception {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Set<String> getInputs() {
		return inputs.keySet();
	}

	@Override
	public void setInput(String input, String value) {
		inputs.put(input, value);
	}

	@Override
	public void setOutput(String output, String value) {
		outputs.put(output, value);
	}

	@Override
	public void incCounter(String name, int value) {

		Integer oldvalue = counters.get(name);
		if (oldvalue == null) {
			oldvalue = 0;
		}
		counters.put(name, oldvalue + value);
		printAndKeep("[INC_COUNTER] " + name + " " + value);
	}

	@Override
	public void submitCounter(String name) {
		printAndKeep("[SUBMIT_COUNTER] " + name);
		submitCounters.put(name, true);
	}

	public Map<String, Integer> getSubmittedCounters() {
		Map<String, Integer> result = new HashMap<String, Integer>();
		for (String counter : submitCounters.keySet()) {
			result.put(counter, counters.get(counter));
		}
		return result;
	}

	@Override
	public Map<String, Integer> getCounters() {
		return counters;
	}

	@Override
	public Object getData(String key) {
		return data.get(key);
	}

	public void setData(String key, Object object) {
		data.put(key, object);
	}

	@Override
	public String createLinkToFile(String id) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getJobName() {
		return jobId;
	}

	public void setHdfsTemp(String hdfsTemp) {
		this.hdfsTemp = hdfsTemp;
	}

	@Override
	public String getHdfsTemp() {
		return hdfsTemp;
	}

	@Override
	public String getLocalTemp() {
		return localTemp;
	}

	public void setLocalTemp(String localTemp) {
		this.localTemp = localTemp;
	}

	@Override
	public void message(String message, int type) {
		printAndKeep("[MESSAGE] [" + getDescription(type) + "] " + message);

	}

	@Override
	public void beginTask(String name) {
		printAndKeep("[BEGIN TASK] " + name);

	}

	@Override
	public void updateTask(String name, int type) {
		printAndKeep("[UPDATE TASK] [" + getDescription(type) + "] " + name);

	}

	@Override
	public void endTask(String message, int type) {
		printAndKeep("[UPDATE TASK] [" + getDescription(type) + "] " + message);
	}

	public String getDescription(int type) {
		switch (type) {
		case 0:
			return "OK";
		case 1:
			return "ERROR";
		case 2:
			return "WARN";
		case 3:
			return "RUN";
		default:
			return "??";
		}
	}

	private void printAndKeep(String text) {
		if (verbose) {
			System.out.println(text.replaceAll("<br>", "\n"));
		}
		memory.append(text + "\n");
	}

	public boolean hasInMemory(String text) {
		return memory.toString().contains(text);
	}

	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}

	public boolean isVerbose() {
		return verbose;
	}

	@Override
	public void setConfig(Map<String, String> config) {
		this.config = config;
	}

	@Override
	public String getConfig(String param) {
		if (config != null) {
			return config.get(param);
		} else {
			return null;
		}
	}
	
	public void setConfig(String param, String value){
		config.put(param, value);
	}

}
