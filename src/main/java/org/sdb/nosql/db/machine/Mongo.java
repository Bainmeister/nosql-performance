package org.sdb.nosql.db.machine;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.sdb.nosql.db.connection.MongoConnection;
import org.sdb.nosql.db.performance.ActionRecord;
import org.sdb.nosql.db.performance.ActionTypes;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.WriteResult;

public class Mongo implements DBMachine {

	DB db;
	DBCollection collection;
	DBCollection log1;
	DBCollection log2;
	DBCollection log3;
	
	public Mongo(MongoConnection connection) {
		db = connection.getDb();
		collection = connection.getCollection();
		log1 = connection.getLog1();
		log2 = connection.getLog2();
		log3 = connection.getLog3();
	}

	// Don't allow creation without a connection
	@SuppressWarnings("unused")
	private Mongo() {

	}

	public ActionRecord read(List<String> keys, int waitMillis) {
		
		final ActionRecord record = new ActionRecord( ActionTypes.READ );
		
		try{
			for (String key : keys){
				collection.findOne(new BasicDBObject("name",key));
				waitBetweenActions(waitMillis);
			}
		} catch (MongoException e) {
			record.setSuccess(false);// most likely a lock failure!
		}
			
		return record;
	}

	public ActionRecord insert(int numberToAdd, int waitMillis) {
		
		final ActionRecord record = new ActionRecord( ActionTypes.INSERT );
		
		// Attempts to make a individual name - this may not be too accurate if
		// there
		// are loads of writes, but I'm not too bothered about this.
		String processNum = System.currentTimeMillis() + "_"
				+ ThreadLocalRandom.current().nextInt(10) + ""
				+ ThreadLocalRandom.current().nextInt(10);

		try{
			for (int i = 1; i < numberToAdd + 1; i++) {
				collection.insert(new BasicDBObject("name", processNum + "_"
						+ String.valueOf(i)).append("value", 0).append("tx", 0));
				
				waitBetweenActions(waitMillis);
			}
		} catch (MongoException e) {
			record.setSuccess(false);// most likely a lock failure!
		}
		return record;
	}

	public ActionRecord update(List<String> keys, int waitMillis) {
		
		final ActionRecord record = new ActionRecord( ActionTypes.UPDATE);
		int n = ThreadLocalRandom.current().nextInt(100);
		
		try{
			for (String key : keys) {
	
				BasicDBObject newDocument = new BasicDBObject();
				newDocument.append("$set",
						new BasicDBObject().append("value", n));
				BasicDBObject searchQuery = new BasicDBObject().append("name", key);
	
				collection.update(searchQuery, newDocument);
	
				waitBetweenActions(waitMillis);
			}
		} catch (MongoException e) {
			record.setSuccess(false);// most likely a lock failure!
		}
		
		return record;
	}

	public ActionRecord balanceTransfer(String key1, String key2, int amount,
			int waitMillis) {
	
		final ActionRecord record = new ActionRecord( ActionTypes.BAL_TRAN);
		
		boolean updateSucceeded = true;

		if ((key1 == null || key1 == "") || (key2 == null || key2 == "")) {
			System.out.println("2 are keys required for balance transfer");
		}

		// Amount to transfer - if the keys are the same don't transfer a thing.
		amount = key1 == key2 ? 0 : amount;
		
		// Setup search querys
		BasicDBObject query1 = new BasicDBObject("name", key1);
		BasicDBObject query2 = new BasicDBObject("name", key2);

		// Query to Decrement balance 1
		BasicDBObject set1 = new BasicDBObject();
		set1.append("$inc", new BasicDBObject().append("value", -amount));

		// Query to Increment balance 2
		BasicDBObject set2 = new BasicDBObject();
		set2.append("$inc", new BasicDBObject().append("value", amount));

		try {

			WriteResult write1 = collection.update(query1, set1); // Update
																	
			waitBetweenActions(waitMillis); // Delay
			
			WriteResult write2 = collection.update(query2, set2); // Update
																	

			updateSucceeded = (write1.getN() == 0) || (write2.getN() == 0) ? false
					: true;

		} catch (MongoException e) {
			record.setSuccess(false);// most likely a lock failure!
		}

		record.setSuccess(updateSucceeded);

		return record;
	}

	public ActionRecord logRead(int waitMillis, int limit) {

		ActionRecord record = new ActionRecord( ActionTypes.READ_LOG);
		
		try{
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
		} catch (MongoException e) {
			record.setSuccess(false);// most likely a lock failure!
		}
		
		return record;
	}

	public ActionRecord logInsert(int waitMillis) {
		ActionRecord record = new ActionRecord(ActionTypes.INSERT_LOG);

		// Attempts to make a individual identifier - this may not be too accurate if
		// there are loads of writes, but I'm not too bothered about this.
		String processNum = System.currentTimeMillis() + "_"
				+ ThreadLocalRandom.current().nextInt(10) + ""
				+ ThreadLocalRandom.current().nextInt(10);
		
		try{
			log1.insert(new BasicDBObject("info", processNum));
			waitBetweenActions(waitMillis);
			log2.insert(new BasicDBObject("info", processNum));
			waitBetweenActions(waitMillis);
			log3.insert(new BasicDBObject("info", processNum));
		} catch (MongoException e) {
			record.setSuccess(false);// most likely a lock failure!
		}

		return record;
	}

	public void waitBetweenActions(int millis) {
		
		if (ThreadLocalRandom.current().nextInt(2)==1 ){
			try {
				TimeUnit.MILLISECONDS.sleep(ThreadLocalRandom.current().nextInt(millis));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	
	}
}
