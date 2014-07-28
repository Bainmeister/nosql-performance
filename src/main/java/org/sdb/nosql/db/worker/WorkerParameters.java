package org.sdb.nosql.db.worker;

public class WorkerParameters {

	public final int dbType = DBTypes.TOKUMX;
	
	public final int threadCount = 1;
	public final int numberOfCalls = 1000;
	public final int batchSize = 10;
	
	public final int chanceOfRead = 1000;
	public final int chanceOfWrite =0;
	public final int chanceOfBalanceTransfer = 0;
	public final int chanceOfReadModifyWrite = 0;
	public final int chanceOfIncrementalUpdate =0; 

	public final int minTransactionSize = 2;
	public final int maxTransactionSize = 2; 

	public final int millisBetweenActions = 0;	
	public final int contendedRecords =3; 	

	public final int writeToLogs = 0;

	public final boolean isCompensator = false;
	public final int COMPENSATE_PROB = 0;
	
}
