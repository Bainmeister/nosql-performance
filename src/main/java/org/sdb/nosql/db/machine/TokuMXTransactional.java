package org.sdb.nosql.db.machine;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.sdb.nosql.db.connection.MongoConnection;
import org.sdb.nosql.db.performance.ActionRecord;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoException;
import com.mongodb.WriteResult;

public class TokuMXTransactional extends TokuMX {
	
	public TokuMXTransactional(MongoConnection connection) {
		super(connection);
	}
	
	@Override
	public ActionRecord read(List<String> keys, int waitMillis) {
		ActionRecord record = new ActionRecord();
		
		db.requestStart();
		try{
			
			db.requestEnsureConnection();
			try {
				//MVCC will grab a snapshot and all the reads should come from the same one.
				db.command(beginTransaction());
				
				for (String key : keys)
					collection.findOne(new BasicDBObject("name",key));
				
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
	public ActionRecord insert(int numberToAdd, int waitMillis) {
		final ActionRecord record = new ActionRecord();

		// Attempts to make a individual name - this may not be too accurate if
		// there
		// are loads of writes, but I'm not too bothered about this.
		final String processNum = System.currentTimeMillis() + "_"
				+ ThreadLocalRandom.current().nextInt(10) + ""
				+ ThreadLocalRandom.current().nextInt(10);
		
		db.requestStart();
		try{
			
			db.requestEnsureConnection();
			try {
				//MVCC will grab a snapshot and all the reads should come from the same one.
				db.command(beginTransaction());
				
				boolean insertSucceeded = true;
				for (int i = 1; i < numberToAdd + 1; i++) {
					collection.insert(new BasicDBObject("name", processNum + "_"
							+ String.valueOf(i)).append("value", 0).append("tx", 0));
				}

				//If either write failed, rollback the transaction.
				db.command(insertSucceeded? commitTransaction() : rollbackTransaction());
				record.setSuccess(insertSucceeded);
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
	public ActionRecord update(List<String> keys, int waitMillis) {
		final ActionRecord record = new ActionRecord();
		
		db.requestStart();
		try{
			
			db.requestEnsureConnection();
			try {
				//MVCC will grab a snapshot and all the reads should come from the same one.
				db.command(beginTransaction());
				boolean updateSucceeded = true;
				for (String key : keys){
					WriteResult write = collection.update(new BasicDBObject("name",key),new BasicDBObject("value",0));
					waitBetweenActions(waitMillis);	
					if (write.getN() == 0)	{
						updateSucceeded =false;
					}
				}
				//If either write failed, rollback the transaction.
				db.command(updateSucceeded? commitTransaction() : rollbackTransaction());
				record.setSuccess(updateSucceeded);
			} catch (MongoException e){
				System.out.println("Updated Lock Failure");
				record.setSuccess(false);
				db.command(rollbackTransaction());
			}
			
		}finally{
			db.requestDone();
		}
		return record;
	}

	@Override
	public ActionRecord balanceTransfer(String key1, String key2, int amount, int waitMillis) {
		final ActionRecord record = new ActionRecord();
		
		boolean updateSucceeded = false;
		
		if ((key1 == null  || key1 =="") || (key2 ==null || key2 =="")){
			System.out.println("2 are keys required for balance transfer");
		}
		
		//Amount to transfer
		int transAmount = key1==key2 ? 0:100;
		
		//Setup search querys
		BasicDBObject query1 = new BasicDBObject("name",key1);
		BasicDBObject query2 = new BasicDBObject("name",key2);
		
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
				
				db.command(beginTransaction());
	
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
	public ActionRecord logRead(int waitMillis, int limit) {
		ActionRecord record = new ActionRecord();
	
		
		
		try {
			db.command(beginTransaction());
			
			
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
			
			db.command(rollbackTransaction());
		}catch (MongoException e){
			record.setSuccess(false);
			db.command(rollbackTransaction());
		}
		
		return record;
	}
	
	@Override
	public ActionRecord logInsert(int waitMillis) {
		ActionRecord record = new ActionRecord();
		
		// Attempts to make a individual identifier - this may not be too accurate if
		// there
		// are loads of writes, but I'm not too bothered about this.
		String processNum = System.currentTimeMillis() + "_"
				+ ThreadLocalRandom.current().nextInt(10) + ""
				+ ThreadLocalRandom.current().nextInt(10);
		
		db.requestStart();
		try{
			
			db.requestEnsureConnection();
			try {
				
				db.command(beginTransaction());
				
				boolean success =true;
				
				WriteResult write1 = log1.insert(new BasicDBObject("info", processNum));
				waitBetweenActions(waitMillis);
				WriteResult write2 = log2.insert(new BasicDBObject("info", processNum));
				waitBetweenActions(waitMillis);
				WriteResult write3 = log3.insert(new BasicDBObject("info", processNum));
				
				if (write1.getN()==0||write2.getN()==0||write3.getN()==0)
					success = false;
				
				
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
	
	BasicDBObject beginTransaction() {
		return beginTransaction("");
	}
	
	BasicDBObject beginTransaction(String isolation){
		//Create beginTransaction object - standard default transaction (MVCC)
		BasicDBObject beginTransaction = new BasicDBObject();
		beginTransaction.append("beginTransaction", 1);
		if (isolation== "serializable" || isolation=="MVCC")
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
