package org.sdb.nosql.db.compensation;


import javax.inject.Inject;
import javax.jws.WebMethod;
import javax.jws.WebService;

import org.jboss.narayana.compensations.api.TransactionCompensatedException;
import org.sdb.nosql.db.compensation.javax.RunnerService;
import org.sdb.nosql.db.connection.MongoConnection;
import org.sdb.nosql.db.performance.ActionRecord;
import org.sdb.nosql.db.worker.Measurement;

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


    private Random rand = new Random(System.currentTimeMillis());

    private int counters;

    @Inject
    private CounterService counterService;

    private double compensateProbability;

    private AtomicInteger compensations = new AtomicInteger(0);

    private DBCollection collection;
    private DBCollection log1;
    private DBCollection log2;
    private DBCollection log3;

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
	
    @WebMethod
    public void setCollections(){
    	MongoConnection conn = new MongoConnection();
    	
    	this.collection =  conn.getCollection();
    	this.log1 = conn.getLog1();
    	this.log2 = conn.getLog2();
    	this.log3 = conn.getLog3();
    	
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
								double compensateProbability, int batchSize, int millisBetween, int logReadLimit){
		
		this.maxTransactionSize = maxTransactionSize;
		this.minTransactionSize = minTransactionSize;
		this.compensateProbability = compensateProbability;
		this.batchSize = batchSize;
		this.millisBetween = millisBetween;
		this.logReadLimit = logReadLimit;
	}

	@Override
	public long run() {

		millisBetween = 0;	
		
		long startMillis = System.currentTimeMillis();
		for (int i = 0; i < batchSize; i++) {
			ActionRecord record = workload();
		}
		long endMillis = System.currentTimeMillis();
		
		return endMillis-startMillis;//endMillis - startMillis;
		
		
	}
	
	ActionRecord workload() {

		ActionRecord record = new ActionRecord();

		List<String> keysToUse = new ArrayList<String>();
		
		
		final int transactionSize = maxTransactionSize == minTransactionSize ? maxTransactionSize
				: ThreadLocalRandom.current().nextInt(maxTransactionSize) + minTransactionSize;
		
		for (int i = 0; i < transactionSize; i++)
			keysToUse.add(availibleKeys.get(rand.nextInt(availibleKeys.size())));

		
		record = logInsert(millisBetween);
		
		// Get Random number to assign task 
		final int rand1 = ThreadLocalRandom.current().nextInt(1000);

		if (rand1 < chanceOfRead) {
			record = read(keysToUse, millisBetween);

		} else if (rand1 < chanceOfInsert) {
			record = insert(transactionSize, millisBetween);

		} else if (rand1 < chanceOfUpdate) {
			record = update(keysToUse, millisBetween);

		} else if (rand1 < chanceOfBalanceTransfer) {
			record = balanceTransfer(keysToUse.get(0),keysToUse.get(1), 10, millisBetween);

		} else if (rand1 < chanceOfLogRead) {
			record = logRead(millisBetween,logReadLimit);

		} else if (rand1 < chanceOfLogInsert) {
			record = logInsert(millisBetween);
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
	
	public ActionRecord update(List<String> keys, int waitMillis) {
		final ActionRecord record = new ActionRecord();
		try {
			counterService.update(keys, compensateProbability, collection, waitMillis);
		} catch (TransactionCompensatedException e) {
			compensations.incrementAndGet();
		}
		return record;
	}
	
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
	
	public ActionRecord logRead(int waitMillis, int limit) {

		ActionRecord record = new ActionRecord();

		if(limit > 0){
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
		// there
		// are loads of writes, but I'm not too bothered about this.
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