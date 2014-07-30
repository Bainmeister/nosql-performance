package org.sdb.nosql.db.worker;

public class WorkerParameters {
	
	
	//These params must not change during a test
	private final int dbType;
	private final boolean isCompensator;
	private final int threadCount;
	private final int numberOfCalls;
	private final int batchSize;
	private final int contendedRecords; 
	
	//It is possible that we may want to change these params during a test - not sure why yet.
	private int chanceOfRead = 0;
	private int chanceOfInsert = 0;
	private int chanceOfUpdate = 0;
	private int chanceOfBalanceTransfer = 0;
	private int chanceOfLogRead= 0;
	private int chanceOfLogInsert = 0; 

	private int minTransactionSize = 0;
	private int maxTransactionSize = 0; 

	private int millisBetweenActions = 0;	
		
	private int writeToLogs = 0;

	public  int COMPENSATE_PROB = 0;
	

	public WorkerParameters(int dbType, boolean isCompensator, int threadCount, int numberOfCalls, int batchSize, int contendedRecords){
		this.dbType = dbType;
		this.isCompensator = isCompensator;
		this.threadCount = threadCount;
		this.numberOfCalls = numberOfCalls;
		this.batchSize = batchSize;
		this.contendedRecords = contendedRecords; 
	}
	
	
	/**
	 * @return the chanceOfRead
	 */
	public int getChanceOfRead() {
		return chanceOfRead;
	}

	/**
	 * @param chanceOfRead the chanceOfRead to set
	 */
	public void setChanceOfRead(int chanceOfRead) {
		this.chanceOfRead = chanceOfRead;
	}

	/**
	 * @return the chanceOfBalanceTransfer
	 */
	public int getChanceOfBalanceTransfer() {
		return chanceOfBalanceTransfer;
	}

	/**
	 * @param chanceOfBalanceTransfer the chanceOfBalanceTransfer to set
	 */
	public void setChanceOfBalanceTransfer(int chanceOfBalanceTransfer) {
		this.chanceOfBalanceTransfer = chanceOfBalanceTransfer;
	}

	/**
	 * @return the minTransactionSize
	 */
	public int getMinTransactionSize() {
		return minTransactionSize;
	}

	/**
	 * @param minTransactionSize the minTransactionSize to set
	 */
	public void setMinTransactionSize(int minTransactionSize) {
		this.minTransactionSize = minTransactionSize;
	}

	/**
	 * @return the maxTransactionSize
	 */
	public int getMaxTransactionSize() {
		return maxTransactionSize;
	}

	/**
	 * @param maxTransactionSize the maxTransactionSize to set
	 */
	public void setMaxTransactionSize(int maxTransactionSize) {
		this.maxTransactionSize = maxTransactionSize;
	}

	/**
	 * @return the millisBetweenActions
	 */
	public int getMillisBetweenActions() {
		return millisBetweenActions;
	}

	/**
	 * @param millisBetweenActions the millisBetweenActions to set
	 */
	public void setMillisBetweenActions(int millisBetweenActions) {
		this.millisBetweenActions = millisBetweenActions;
	}

	/**
	 * @return the writeToLogs
	 */
	public int getWriteToLogs() {
		return writeToLogs;
	}

	/**
	 * @param writeToLogs the writeToLogs to set
	 */
	public void setWriteToLogs(int writeToLogs) {
		this.writeToLogs = writeToLogs;
	}

	/**
	 * @return the threadCount
	 */
	public int getThreadCount() {
		return threadCount;
	}

	/**
	 * @return the numberOfCalls
	 */
	public int getNumberOfCalls() {
		return numberOfCalls;
	}

	/**
	 * @return the batchSize
	 */
	public int getBatchSize() {
		return batchSize;
	}

	/**
	 * @return the contendedRecords
	 */
	public int getContendedRecords() {
		return contendedRecords;
	}

	/**
	 * @return the cOMPENSATE_PROB
	 */
	public int getCOMPENSATE_PROB() {
		return COMPENSATE_PROB;
	}
	
	public int getDbType() {
		return dbType;
	}

	public boolean isCompensator() {
		return isCompensator;
	}


	public int getChanceOfInsert() {
		return chanceOfInsert;
	}


	public void setChanceOfInsert(int chanceOfInsert) {
		this.chanceOfInsert = chanceOfInsert;
	}


	public int getChanceOfUpdate() {
		return chanceOfUpdate;
	}


	public void setChanceOfUpdate(int chanceOfUpdate) {
		this.chanceOfUpdate = chanceOfUpdate;
	}


	public int getChanceOfLogRead() {
		return chanceOfLogRead;
	}


	public void setChanceOfLogRead(int chanceOfLogRead) {
		this.chanceOfLogRead = chanceOfLogRead;
	}


	public int getChanceOfLogInsert() {
		return chanceOfLogInsert;
	}


	public void setChanceOfLogInsert(int chanceOfLogInsert) {
		this.chanceOfLogInsert = chanceOfLogInsert;
	}

	
}
