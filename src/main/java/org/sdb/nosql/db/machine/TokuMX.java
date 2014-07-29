package org.sdb.nosql.db.machine;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.bson.BasicBSONObject;
import org.sdb.nosql.db.connection.MongoConnection;
import org.sdb.nosql.db.performance.ActionRecord;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.WriteResult;

public class TokuMX implements DBMachine {

	DB db;
	DBCollection collection;
	
	public TokuMX(MongoConnection connection){
		db = connection.getDb();
		collection = connection.getCollection();
	}

	@SuppressWarnings("unused")
	private TokuMX(){
		
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

	public ActionRecord update(List<String> keys, int waitMillis) {
		final ActionRecord record = new ActionRecord();
		
		for (String key : keys){
			//ObjectId keyObj = new ObjectId(key);
			
			
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

	public ActionRecord insert(List<String> values, int waitMillis) {
		// TODO Auto-generated method stub
		return null;
	}
	
	public ActionRecord readModifyWrite(List<String> keys, int waitMillis) {
		final ActionRecord record = new ActionRecord();
		
		for (String key : keys){
			
			BasicDBObject newDocument = new BasicDBObject().append("$inc", new BasicDBObject().append("increment", 1));
	 
			collection.update(new BasicDBObject().append("name", key), newDocument);
			
			//Wait for millis
			if (waitMillis > 0);
				waitBetweenActions(waitMillis);
		}
		return record;
	}
	
	public ActionRecord incrementalUpdate(List<String> keys, int waitMillis) {
		final ActionRecord record = new ActionRecord();
		

		for (String key :keys){
			//Create usable keys
			//ObjectId keyObj1 = new ObjectId(key);
			
			//Setup a search query
			BasicDBObject searchQuery1 = new BasicDBObject("name",key);
			
			//Set the element to return
			BasicDBObject fields = new BasicDBObject();
			fields.put("increment", 1);
			
			//Get the current value from the db
			DBObject doc1 = collection.findOne(searchQuery1, fields);
			int doc1Mod = (Integer) doc1.get("increment");
			
			//Wait for millis
			if (waitMillis > 0);
				waitBetweenActions(waitMillis);
			
			//Add 1 to the value
			BasicDBObject newDocument = new BasicDBObject();
			newDocument.append("$set", new BasicDBObject().append("increment", doc1Mod +1));
			BasicDBObject searchQuery = new BasicDBObject().append("name",key);
			collection.update(searchQuery, newDocument);	
		}
		
			
		
		return record;
	}

	public ActionRecord balanceTransfer(String key1, String key2, int waitMillis) {
		final ActionRecord record = new ActionRecord();
		
		boolean updateSucceeded = true;
		
		if ((key1 == null  || key1 =="") || (key2 ==null || key2 =="")){
			System.out.println("2 are keys required for balance transfer");
		}
		
		//Amount to transfer
		int transAmount = key1==key2 ? 0:100;
		
		//Create usable keys from key Strings
		//ObjectId keyObj1 = new ObjectId(key1);
		//ObjectId keyObj2 = new ObjectId(key2);
		
		//Setup search querys
		BasicDBObject query1 = new BasicDBObject("name",key1);
		BasicDBObject query2 = new BasicDBObject("name",key2);
		
		//Query to Decrement balance 1
		BasicDBObject set1 = new BasicDBObject();
		set1.append("$inc", new BasicDBObject().append("value", -transAmount));
	
		//Query to Increment balance 2
		BasicDBObject set2 = new BasicDBObject();
		set2.append("$inc", new BasicDBObject().append("value", transAmount));
		
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

	public void addTable(String name) {
		db.getCollection(name);
	}

	public ActionRecord writeLog(int numberToWrite, int waitMillis) {
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

	public ActionRecord readLog(int numberToRead , int waitMillis) {
		
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
	
	
	BasicDBObject beginTransaction(String isolation){
		
		//Create beginTransaction object
		BasicDBObject beginTransaction = new BasicDBObject();
		beginTransaction.append("beginTransaction", 1);
		
		if (isolation == "MVCC" || isolation == "serializable")
			beginTransaction.append("isolation", isolation);
		
		return beginTransaction;
	}

	BasicDBObject rollbackTransaction(){
		//Create rollbackTransaction object
		BasicDBObject rollbackTransaction = new BasicDBObject();
		rollbackTransaction.append("rollbackTransaction", 1);
		return rollbackTransaction;
	}
	
	BasicDBObject commitTransaction(){
		//Create rollbackTransaction object
		BasicDBObject commitTransaction = new BasicDBObject();
		commitTransaction.append("commitTransaction", 1);
		return commitTransaction;
	}
	
}
