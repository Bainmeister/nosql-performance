package org.sdb.nosql.db.worker;

public class Measurement {

	private long failed = 0;
	private long timeTaken = 0;
	private long successful = 0;
	private long callNumber = 0; 
	

	public long getSuccessful() {
		return successful;
	}

	public long getCallNumber() {
		return callNumber;
	}
	public long getErrorCount() {
		return failed;
	}
	public long getTimeTaken() {
		return timeTaken;
	}
	
	public void addToMeasuement(int callNumber, int successful, int failed,  long timeTaken) {
		
		this.callNumber = this.callNumber + callNumber;
		this.successful = this.successful + successful;
		this.failed = this.failed + failed;
		this.timeTaken = this.timeTaken + timeTaken;
	}
}
