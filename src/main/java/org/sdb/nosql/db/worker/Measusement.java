package org.sdb.nosql.db.worker;

public class Measusement {

	private long errorCount;
	private long timeTaken;
	private long successful;
	private long callNumber; 
	
	public long getErrorCount() {
		return errorCount;
	}
	public void setErrorCount(int errorCount) {
		this.errorCount = errorCount;
	}
	public long getTimeTaken() {
		return timeTaken;
	}
	public void setTimeTaken(long timeTaken) {
		this.timeTaken = timeTaken;
	}
	public void incrementErrorCount() {
		errorCount++;
	}
	public void incrementgetSuccessful() {
		successful++;
	}
	public long getSuccessful() {
		return successful;
	}
	public void incrementCallNumber() {
		setCallNumber(getCallNumber() + 1);
	}
	public long getCallNumber() {
		return callNumber;
	}
	public void setCallNumber(long callNumber) {
		this.callNumber = callNumber;
	}
	
}
