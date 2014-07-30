package org.sdb.nosql.db.machine;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.sdb.nosql.db.connection.MongoConnection;
import org.sdb.nosql.db.performance.ActionRecord;

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
	List<DBCollection> logCollections;
	
	public Mongo(MongoConnection connection){
		db = connection.getDb();
		collection = connection.getCollection();
		logCollections = connection.getLogCollections();
	}
	
	//Don't allow creation without a connection
	@SuppressWarnings("unused")
	private Mongo(){
		
	}
	
	public ActionRecord read(List<String> keys, int waitMillis) {
		final ActionRecord record = new ActionRecord();
		
		for (String key : keys){
			BasicDBObject searchQuery = new BasicDBObject("name",key);

			DBCursor cursor = collection.find(searchQuery);

			try{
				while(cursor.hasNext()) {
					cursor.next();
				}
			}finally{
				cursor.close();
			}
			
			//Wait for millis
			if (waitMillis > 0);
				waitBetweenActions(waitMillis);
			
		}
		
		return record;
	}

	public ActionRecord insert(int numberToAdd, int waitMillis) {
		final ActionRecord record = new ActionRecord();
		
		//Attempts to make a individual name - this may not be too accurate if there 
		// are loads of writes, but I'm not too bothered about this. 
		String processNum =  System.currentTimeMillis() +"_"+
								ThreadLocalRandom.current().nextInt(10) + "" +
								ThreadLocalRandom.current().nextInt(10);
	
		for (int i=1; i < numberToAdd+1; i++) {
			collection.insert(new BasicDBObject("name", processNum + "_"+String.valueOf(i)).append("value", 0).append("tx", 0));
		}
		
		return record;
	}
	
	public ActionRecord update(List<String> keys, int waitMillis) {
		final ActionRecord record = new ActionRecord();
		
		for (String key : keys){

			BasicDBObject newDocument = new BasicDBObject();
			newDocument.append("$set", new BasicDBObject().append("balance", 200));
			BasicDBObject searchQuery = new BasicDBObject().append("name",key);
		 
			collection.update(searchQuery, newDocument);
			
			//Wait for millis
			if (waitMillis > 0);
				waitBetweenActions(waitMillis);
		}
		
		
		return record;
	}
	
	public ActionRecord balanceTransfer(String key1, String key2, int amount, int waitMillis) {
		final ActionRecord record = new ActionRecord();
		
		boolean updateSucceeded = true;
		
		if ((key1 == null  || key1 =="") || (key2 ==null || key2 =="")){
			System.out.println("2 are keys required for balance transfer");
		}
		
		//Amount to transfer - if the keys are the same don't transfer a thing.
		amount = key1==key2 ? 0:amount;
		
		//Setup search querys
		BasicDBObject query1 = new BasicDBObject("name",key1);
		BasicDBObject query2 = new BasicDBObject("name",key2);
		
		//Query to Decrement balance 1
		BasicDBObject set1 = new BasicDBObject();
		set1.append("$inc", new BasicDBObject().append("value", -amount));
	
		//Query to Increment balance 2
		BasicDBObject set2 = new BasicDBObject();
		set2.append("$inc", new BasicDBObject().append("value", amount));
		
		try{
			
			WriteResult write1 = collection.update(query1, set1);  	//Update record 1
			waitBetweenActions(waitMillis);							//Delay
			WriteResult write2 = collection.update(query2, set2);	//Update record 2
					
			updateSucceeded = (write1.getN() == 0)||(write2.getN() == 0)? false:true;
				
		}catch (MongoException e){
			record.setSuccess(false);// most likely a lock failure!
		}
		
		
		record.setSuccess(updateSucceeded);
		
		
		return record;
	}

public ActionRecord logRead(int numberToRead , int waitMillis) {
		
		ActionRecord record = new ActionRecord();
		
		for (int i = 0; i < numberToRead; i++){
			DBCollection log = db.getCollection("log"+i);
			DBCursor cursor = log.find().limit(1000);
			while (cursor.hasNext()) {
				//Scan through the log!
				cursor.next();
			}
			
			//Wait for millis
			if (waitMillis > 0);
				waitBetweenActions(waitMillis);
		
		}
		
		return record;
	}

	public void waitBetweenActions(int millis){
		try {
			TimeUnit.MILLISECONDS.sleep(millis);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}	
	}
	
	public ActionRecord logInsert(int numberToWrite, int waitMillis) {
		ActionRecord record = new ActionRecord();
		
		for (int i = 0; i<numberToWrite; i++){
			DBCollection log = db.getCollection("log"+i);
			log.insert(new BasicDBObject("log", i));
			
			//Wait for millis
			if (waitMillis > 0);
				waitBetweenActions(waitMillis);
		}
		
		return record;
	}

	
}
