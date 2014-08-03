package org.sdb.nosql.db.compensation;


import javax.inject.Inject;
import javax.jws.WebMethod;
import javax.jws.WebService;

import org.jboss.narayana.compensations.api.TransactionCompensatedException;
import org.sdb.nosql.db.compensation.javax.RunnerService;
import org.sdb.nosql.db.connection.MongoConnection;
import org.sdb.nosql.db.performance.ActionRecord;
import org.sdb.nosql.db.worker.DBTypes;
import org.sdb.nosql.db.worker.DBWorker;
import org.sdb.nosql.db.worker.Measurement;
import org.sdb.nosql.db.worker.WorkerParameters;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author paul.robinson@redhat.com 10/07/2014
 */
@WebService(serviceName = "HotelServiceService", portName = "HotelService", name = "HotelService", targetNamespace = "http://www.jboss.org/as/quickstarts/compensationsApi/travel/hotel")
public class RunnerServiceImpl implements RunnerService {

    private int counters;

    @Inject
    private CounterService counterService;

    private double compensateProbability;

 
	private List<String> availibleKeys;
	
	private int chanceOfRead;
	private int chanceOfInsert;
	private int chanceOfUpdate;
	private int chanceOfBalanceTransfer;
	private int chanceOfLogRead;
	private int chanceOfLogInsert;

	private int maxTransactionSize;
	private int minTransactionSize;
	private int batchSize;
	private int millisBetween;
	private int logReadLimit;

	
	private long totalRunTime =0;
	private long numberOfCalls= 0;

	private int contendedRecords;
	
	/**
	 * @return the totalRunTime
	 */
	@Override
	@WebMethod
	public long getTotalRunTime() {
		return totalRunTime;
	}

	/**
	 * @return the numberOfCalls
	 */
	@Override
	@WebMethod
	public long getNumberOfCalls() {
		return numberOfCalls;
	}
	   
	@Override
	public void setContendedRecords(List<String> availibleKeys) {
		this.availibleKeys = availibleKeys;
	}
	
	@Override
	@WebMethod
	public void setChances(int chanceOfRead,
			int chanceOfInsert, int chanceOfUpdate,
			int chanceOfBalanceTransfer, int chanceOfLogRead,
			int chanceOfLogInsert){
		
		this.chanceOfRead = chanceOfRead;
		this.chanceOfInsert = chanceOfInsert;
		this.chanceOfUpdate = chanceOfUpdate;
		this.chanceOfBalanceTransfer = chanceOfBalanceTransfer;
		this.chanceOfLogRead = chanceOfLogRead;
		this.chanceOfLogInsert = chanceOfLogInsert; 
	}
	
	@Override
	@WebMethod
	public void setParams(int maxTransactionSize, int minTransactionSize,
								double compensateProbability, int batchSize, int millisBetween, int logReadLimit, int contendedRecords){
		
		this.contendedRecords = contendedRecords;
		this.maxTransactionSize = maxTransactionSize;
		this.minTransactionSize = minTransactionSize;
		this.compensateProbability = compensateProbability;
		this.batchSize = batchSize;
		this.millisBetween = millisBetween;
		this.logReadLimit = logReadLimit;
	}

	@Override
	@WebMethod
	public void run(long runTime) {
		
		//Annoyingly have to recreate the parameter object on this side now. 
		WorkerParameters params = new WorkerParameters(DBTypes.TOKUMX,true,0, batchSize, contendedRecords );
		params.setChanceOfRead(chanceOfRead);
		params.setChanceOfInsert(chanceOfInsert);
		params.setChanceOfUpdate(chanceOfUpdate);
		params.setChanceOfBalanceTransfer(chanceOfBalanceTransfer);
		params.setChanceOfLogRead(chanceOfLogRead);
		params.setChanceOfLogInsert(chanceOfLogInsert);
		
		params.setMinTransactionSize(minTransactionSize);
		params.setMaxTransactionSize(maxTransactionSize);
		params.setMillisBetweenActions(millisBetween);
		params.setLogReadLimit(logReadLimit);


		//Ok now call the DB worker from this side of the Web call. 
		DBWorker worker = new DBWorker(availibleKeys,params);
		
		Measurement m = worker.doWork(runTime);
		numberOfCalls = m.getCallNumber();
		totalRunTime = m.getTimeTaken();
		
		

	}

	

}