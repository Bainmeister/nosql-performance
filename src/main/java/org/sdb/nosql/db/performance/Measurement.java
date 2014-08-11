package org.sdb.nosql.db.performance;

public class Measurement {

	private long failed = 0;
	private long timeTaken = 0;
	private long successful = 0;
	private long callNumber = 0;

	private long successfulReads = 0;
	private long failedReads = 0;
	private long successfulInserts = 0;
	private long failedInserts = 0;
	private long successfulUpdates = 0;
	private long failedUpdates = 0;
	private long successfulBalTrans = 0;
	private long failedBalTrans = 0;
	private long failedLogReads = 0;
	private long failedLogWrites = 0;
	private long successfulLogWrites = 0;
	private long successfulLogReads = 0; 
	
	
	public void addToMeasuement(int callNumber, int action,  int successful, int failed,  long timeTaken) {
		
		///OVERALL
		this.callNumber = this.callNumber + callNumber;
		this.successful = this.successful + successful;
		this.failed = this.failed + failed;
		this.timeTaken = this.timeTaken + timeTaken;
		
		//Specific setting of values
		if (action == ActionTypes.READ){
			
			this.successfulReads = this.successfulReads + successful;
			this.failedReads = this.failedReads + failed;
			
		} else if (action == ActionTypes.INSERT){
			
			this.successfulInserts = this.successfulInserts + successful;
			this.failedInserts = this.failedInserts + failed;
			
		} else if (action == ActionTypes.UPDATE){
			
			this.successfulUpdates = this.successfulUpdates + successful;
			this.failedUpdates = this.failedUpdates + failed;
			
		} else if (action == ActionTypes.BAL_TRAN){
			
			this.successfulBalTrans = this.successfulBalTrans + successful;
			this.failedBalTrans = this.failedBalTrans + failed;
			
		} else if (action == ActionTypes.READ_LOG){
			
			this.successfulLogReads = this.successfulLogReads + successful;
			this.failedLogReads = this.failedLogReads + failed;
			
		} else if (action == ActionTypes.INSERT_LOG){
		
			this.successfulLogWrites = this.successfulLogWrites + successful;
			this.failedLogWrites = this.failedLogWrites + failed;
			
		}
	
	}
	
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
	
	public long getSuccessfulReads() {
		return successfulReads;
	}

	public long getFailedReads() {
		return failedReads;
	}

	public long getSuccessfulInserts() {
		return successfulInserts;
	}

	public long getFailedInserts() {
		return failedInserts;
	}

	public long getSuccessfulUpdates() {
		return successfulUpdates;
	}
	public long getFailedUpdates() {
		return failedUpdates;
	}

	public long getSuccessfulBalTrans() {
		return successfulBalTrans;
	}

	public long getFailedBalTrans() {
		return failedBalTrans;
	}

	public long getFailedLogReads() {
		return failedLogReads;
	}

	public long getFailedLogWrites() {
		return failedLogWrites;
	}

	public long getSuccessfulLogWrites() {
		return successfulLogWrites;
	}

	public long getSuccessfulLogReads() {
		return successfulLogReads;
	}

	public void setSuccessfulUpdates(long successfulUpdates) {
		this.successfulUpdates = successfulUpdates;
	}
	
}
