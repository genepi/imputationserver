package genepi.imputationserver.util;

import org.apache.commons.logging.Log;

import cloudgene.sdk.internal.WorkflowContext;

public class ContextLog implements Log {

	private WorkflowContext context; 
	
	public ContextLog(WorkflowContext context){
		this.context = context;
	}
	
	@Override
	public boolean isDebugEnabled() {
		return false;
	}

	@Override
	public boolean isErrorEnabled() {
		return false;
	}

	@Override
	public boolean isFatalEnabled() {
		return false;
	}

	@Override
	public boolean isInfoEnabled() {
		return false;
	}

	@Override
	public boolean isTraceEnabled() {
		return false;
	}

	@Override
	public boolean isWarnEnabled() {
		return false;
	}

	@Override
	public void trace(Object message) {
		// TODO Auto-generated method stub

	}

	@Override
	public void trace(Object message, Throwable t) {
		// TODO Auto-generated method stub

	}

	@Override
	public void debug(Object message) {
		context.println("[DEBUG] " + message.toString());

	}

	@Override
	public void debug(Object message, Throwable t) {
		
	}

	@Override
	public void info(Object message) {
		context.println("[INFO] " + message.toString());
	}

	@Override
	public void info(Object message, Throwable t) {
		// TODO Auto-generated method stub

	}

	@Override
	public void warn(Object message) {
		context.println("[WARNING] " + message.toString());
	}

	@Override
	public void warn(Object message, Throwable t) {
		// TODO Auto-generated method stub

	}

	@Override
	public void error(Object message) {
		context.println("[ERROR] " + message.toString());

	}

	@Override
	public void error(Object message, Throwable t) {
		// TODO Auto-generated method stub

	}

	@Override
	public void fatal(Object message) {
		context.println("[FATAL] " + message.toString());

	}

	@Override
	public void fatal(Object message, Throwable t) {
		// TODO Auto-generated method stub

	}

}
