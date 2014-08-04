package org.sdb.nosql.db.compensation;


import javax.inject.Inject;
import javax.jws.WebMethod;
import javax.jws.WebService;

import org.jboss.narayana.compensations.api.TransactionCompensatedException;
import org.sdb.nosql.db.compensation.javax.RunnerService;
import org.sdb.nosql.db.connection.MongoConnection;
import org.sdb.nosql.db.performance.ActionRecord;
import org.sdb.nosql.db.worker.DBTypes;
import org.sdb.nosql.db.worker.Measurement;
import org.sdb.nosql.db.worker.WorkerParameters;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author paul.robinson@redhat.com 10/07/2014
 */
@WebService(serviceName = "HotelServiceService", portName = "HotelService", name = "HotelService", targetNamespace = "http://www.jboss.org/as/quickstarts/compensationsApi/travel/hotel")
public class RunnerServiceImpl implements RunnerService {


    private AtomicInteger compensations = new AtomicInteger(0);

    private double compensateProbability;

    private WorkerParameters params;
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
	
	DB db;
	DBCollection collection;
	DBCollection log1;
	DBCollection log2;
	DBCollection log3;
	
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
		
		
		//Annoyingly have to recreate the parameter object on this side now. 
		params = new WorkerParameters(DBTypes.TOKUMX,true,0, batchSize, contendedRecords );
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
		
		MongoConnection connection = new MongoConnection();
		db = connection.getDb();
		collection = connection.getCollection();
		log1 = connection.getLog1();
		log2 = connection.getLog2();
		log3 = connection.getLog3();
		
		//Ok now call the DB worker from this side of the Web call. 
		//worker = new DBWorker(availibleKeys,params);
	}

	@Override
	@WebMethod
	public void run() {
		
		//TODO this would be great if we could use a DB worker.
		Measurement m = doWork();
		numberOfCalls = m.getCallNumber();
		totalRunTime = m.getTimeTaken();

	}

	public Measurement doWork() {
		
		int batchSize = params.getBatchSize();
		Measurement measurement = new Measurement();
			
		ActionRecord record = null;
				
		//Do the work get the measurements
		for (int i = 0 ; i < batchSize;i++){
			
			//////////////RUN THE WORKLOAD///////////////////
			long startTimeMillis = System.currentTimeMillis();
			record = workload();
			long endTimeMillis = System.currentTimeMillis();
			//////////////RUN THE WORKLOAD///////////////////
			
			boolean success = (record.isSuccess())?true:false;
			boolean failed = ( record ==null || !record.isSuccess() || record.isDataFailue() || record.isLockFailure() )? true:false ;
	    	measurement.addToMeasuement(1, success?1:0, failed?1:0, endTimeMillis-startTimeMillis);
	    	
		}
	
    	
		return measurement;
	}
	
    private ActionRecord workload( ) {
    	ActionRecord record = new ActionRecord();
    	    	
    	final int transactionSize = maxTransactionSize == minTransactionSize ? maxTransactionSize :
    		ThreadLocalRandom.current().nextInt(maxTransactionSize)+minTransactionSize; 
    	
    	List<String> keysToUse = getKeysForTransaction(transactionSize); 
    	
    	//Get Random number to assign task
    	final int rand1 = ThreadLocalRandom.current() .nextInt(1000);
    	
    	if (rand1< chanceOfRead){
    		record = read(keysToUse,millisBetween);
    		
    	}else if(rand1 < chanceOfInsert){
        		record = insert(transactionSize, millisBetween);	
    
    	}else if(rand1 < chanceOfUpdate){
    		record = update(keysToUse, millisBetween);	
    	      	
    	}else if (rand1 < chanceOfBalanceTransfer){
    		
    		record = balanceTransfer(keysToUse.get(0), keysToUse.get(1),10 , millisBetween);
    	
    	}else if (rand1 < chanceOfLogRead){
    		record = logRead(millisBetween, logReadLimit);
    		
    	}else if (rand1 < chanceOfLogInsert){
    		record = logInsert( millisBetween);
    	}
		return record;
	}
    
    private List<String> getKeysForTransaction(int numberToGet){

    	List<String>  keys= new ArrayList<String> ();
    	List<Integer>  used= new ArrayList<Integer> ();
    	
    	if	( availibleKeys ==null){
    		System.out.println("Keys have not setup");
    		numberToGet =  0;
    	}
    	//If the transaction is too large, reduce it to a size we can hanle.
    	if(numberToGet > availibleKeys.size()){
    		System.out.println("Warning! Transaction size too large - reducing to "+availibleKeys.size());
    		numberToGet =  availibleKeys.size();
    	}
    	
    	while(keys.size() < numberToGet){
    		int recordAt = ThreadLocalRandom.current().nextInt(availibleKeys.size()); 
    		
    		if(!used.contains(recordAt)){
    			used.add(recordAt);
    			keys.add(availibleKeys.get(recordAt));
    		}
    	}
    	
    	return keys;
    }

    
    @Inject
    private CounterService counterService;
	// Reads will be the same a Mongo, however any writes/updates need to go via
	// the
	// Compensation methods
	public ActionRecord balanceTransfer(String key1, String key2, int amount,
			int waitMillis) {
		ActionRecord record = new ActionRecord();	
		
		try {
			counterService.updateCounters( key1, key2, amount, compensateProbability, collection, waitMillis);
		} catch (TransactionCompensatedException e) {
			compensations.incrementAndGet();
		}
		
		return record;
	}

	public ActionRecord update(List<String> keys, int waitMillis) {
		final ActionRecord record = new ActionRecord();
		try {
			counterService.update(keys, compensateProbability, collection, waitMillis);
		} catch (TransactionCompensatedException e) {
			compensations.incrementAndGet();
		}
		return record;
	}
	
	public ActionRecord read(List<String> keys, int waitMillis) {
		
		final ActionRecord record = new ActionRecord();
		for (String key : keys){
			collection.findOne(new BasicDBObject("name",key));
			waitBetweenActions(waitMillis);
		}

		return record;
	}

	public ActionRecord insert(int numberToAdd, int waitMillis) {
		final ActionRecord record = new ActionRecord();
		
		// Attempts to make a individual name - this may not be too accurate if
		// there
		// are loads of writes, but I'm not too bothered about this.
		String processNum = System.currentTimeMillis() + "_"
				+ ThreadLocalRandom.current().nextInt(10) + ""
				+ ThreadLocalRandom.current().nextInt(10);

		for (int i = 1; i < numberToAdd + 1; i++) {
			collection.insert(new BasicDBObject("name", processNum + "_"
					+ String.valueOf(i)).append("value", 0).append("tx", 0));
			
			waitBetweenActions(waitMillis);
		}

		return record;
	}
	
	public ActionRecord logRead(int waitMillis, int limit) {

		ActionRecord record = new ActionRecord();
		
		if(limit>0){
			log1.find().limit(limit);
			waitBetweenActions(waitMillis);
			log2.find().limit(limit);
			waitBetweenActions(waitMillis);
			log3.find().limit(limit);
		}else{
			log1.find();
			waitBetweenActions(waitMillis);
			log2.find();
			waitBetweenActions(waitMillis);
			log3.find();
		}
		
		return record;
	}

	public ActionRecord logInsert(int waitMillis) {
		ActionRecord record = new ActionRecord();

		// Attempts to make a individual identifier - this may not be too accurate if
		// there are loads of writes, but I'm not too bothered about this.
		String processNum = System.currentTimeMillis() + "_"
				+ ThreadLocalRandom.current().nextInt(10) + ""
				+ ThreadLocalRandom.current().nextInt(10);
		
		
		log1.insert(new BasicDBObject("info", processNum));
		waitBetweenActions(waitMillis);
		log2.insert(new BasicDBObject("info", processNum));
		waitBetweenActions(waitMillis);
		log3.insert(new BasicDBObject("info", processNum));


		return record;
	}

	public void waitBetweenActions(int millis) {
		
		if (ThreadLocalRandom.current().nextInt(2)==1 ){
			try {
				TimeUnit.MILLISECONDS.sleep(millis);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	
	}
}