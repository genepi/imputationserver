package genepi.imputationserver.steps.fastqc;

public interface ITask {

	public String getName();
	
	public TaskResults run(ITaskProgressListener progressListener) throws Exception;

}
