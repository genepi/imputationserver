package genepi.imputationserver.steps.fastqc;

public interface ITask {

	public String getName();
	
	public TaskResult run(ITaskProgressListener progressListener) throws Exception;

}
