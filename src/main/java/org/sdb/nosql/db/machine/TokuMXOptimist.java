package org.sdb.nosql.db.machine;

import java.util.ArrayList;
import java.util.List;

import org.bson.types.ObjectId;
import org.sdb.nosql.db.connection.MongoConnection;
import org.sdb.nosql.db.performance.ActionRecord;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.MongoException;
import com.mongodb.WriteResult;

public class TokuMXOptimist extends TokuMX {

	public TokuMXOptimist(MongoConnection connection) {
		super(connection);
	}
	

	@Override
	public ActionRecord read(List<String> keys, int waitMillis) {
		ActionRecord record = new ActionRecord();
		//Create usable keys from key Strings
		List<ObjectId> keyList = new ArrayList<ObjectId>();
		for (String key: keys)
			keyList.add( new ObjectId(key));
		
		db.requestStart();
		try{
			
			db.requestEnsureConnection();
			try {
				//MVCC will grab a snapshot and all the reads should come from the same one.
				db.command(beginTransaction("mvcc"));
				for (ObjectId key : keyList)
					collection.find(new BasicDBObject("_id",key));
				db.command(rollbackTransaction());
				
			} catch (MongoException e){
				record.setSuccess(false);
			}
			
		}finally{
			db.requestDone();
		}
		return record;
	}

	@Override
	public ActionRecord update(List<String> keys, int waitMillis) {
		final ActionRecord record = new ActionRecord();
		
		
		//Create usable keys from key Strings
		List<ObjectId> keyList = new ArrayList<ObjectId>();
		for (String key: keys)
			keyList.add( new ObjectId(key));
		
		db.requestStart();
		try{
			
			db.requestEnsureConnection();
			try {
				//MVCC will grab a snapshot and all the reads should come from the same one.
				db.command(beginTransaction("mvcc"));
				boolean updateSucceeded = true;
				for (ObjectId key : keyList){
					WriteResult write = collection.update(new BasicDBObject("_id",key),new BasicDBObject("value",2000));
					waitBetweenActions(waitMillis);	
					if (write.getN() == 0)	
						updateSucceeded =false;
				}
				//If either write failed, rollback the transaction.
				db.command(updateSucceeded? commitTransaction() : rollbackTransaction());
				record.setSuccess(updateSucceeded);
			} catch (MongoException e){
				record.setSuccess(false);
				db.command(rollbackTransaction());
			}
			
		}finally{
			db.requestDone();
		}
		return record;
	}

	@Override
	public ActionRecord balanceTransfer(String key1, String key2, int waitMillis) {
		final ActionRecord record = new ActionRecord();
		
		boolean updateSucceeded = false;
		
		if ((key1 == null  || key1 =="") || (key2 ==null || key2 =="")){
			System.out.println("2 are keys required for balance transfer");
		}
		
		//Amount to transfer
		int transAmount = key1==key2 ? 0:100;
		
		//Create usable keys from key Strings
		ObjectId keyObj1 = new ObjectId(key1);
		ObjectId keyObj2 = new ObjectId(key2);
		
		//Setup search querys
		BasicDBObject query1 = new BasicDBObject("_id",keyObj1);
		BasicDBObject query2 = new BasicDBObject("_id",keyObj2);
		
		//Query to Decrement balance 1
		BasicDBObject set1 = new BasicDBObject();
		set1.append("$inc", new BasicDBObject().append("value", -transAmount));
	
		//Query to Increment balance 2
		BasicDBObject set2 = new BasicDBObject();
		set2.append("$inc", new BasicDBObject().append("value", transAmount));
		
		db.requestStart();
		try{
			db.requestEnsureConnection();
			
			
			//***** TRANSACTION****//
			try{
				
				db.command(beginTransaction("mvcc"));
	
				WriteResult write1 = collection.update(query1, set1);  	//Update record 1
				waitBetweenActions(waitMillis);							//Delay
				WriteResult write2 = collection.update(query2, set2);	//Update record 2
				waitBetweenActions(waitMillis);							//Delay
					
				updateSucceeded = (write1.getN() == 0)||(write2.getN() == 0)? false:true;
				
				//If either write failed, rollback the transaction.
				db.command(updateSucceeded? commitTransaction() : rollbackTransaction());
				
				//IMPORTANT: TOKUMX appears not to handle MVCC correctly - any transaction that completes inside the time of a 
				//running transaction will be overwritten.  I have asked TOKUMX about this - no valid response yet. 
				
			}catch (MongoException e){
				record.setSuccess(false);// most likely a lock failure!
				db.command(rollbackTransaction());
			}
			//***** TRANSACTION OVER ****//
			
			record.setSuccess(updateSucceeded);
		}finally{
			db.requestDone();
		}
		return record;
	}

	@Override
	public ActionRecord writeLog(int numberToWrite, int waitMillis) {
		ActionRecord record = new ActionRecord();
		
		db.requestStart();
		try{
			
			db.requestEnsureConnection();
			try {
				//MVCC will grab a snapshot and all the reads should come from the same one.
				db.command(beginTransaction("mvcc"));
				boolean success =true;
				for (int i = 0; i<numberToWrite; i++){
					WriteResult write2 = db.getCollection("log"+i).insert(new BasicDBObject("log", i));
					waitBetweenActions(waitMillis);
					if (write2.getN()==0)
						success = false;
				}
				
				db.command(success==false?rollbackTransaction():commitTransaction());
				
			} catch (MongoException e){
				db.command(rollbackTransaction());
				record.setSuccess(false);
			}
			
		}finally{
			db.requestDone();
		}
		return record;
	}

	@Override
	public ActionRecord readLog(int numberToRead, int waitMillis) {
		ActionRecord record = new ActionRecord();
				
		try {
			db.command(beginTransaction("mvcc"));
			for (int i = 0; i < numberToRead; i++){
				
				DBCollection log = db.getCollection("log"+i);
				log.find().limit(1000);
				
				waitBetweenActions(waitMillis);
			
			}
			db.command(rollbackTransaction());
		}catch (MongoException e){
			record.setSuccess(false);
			db.command(rollbackTransaction());
		}
		
		return record;
	}

}
