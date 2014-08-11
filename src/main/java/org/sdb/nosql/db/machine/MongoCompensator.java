package org.sdb.nosql.db.machine;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import javax.inject.Inject;

import org.jboss.narayana.compensations.api.TransactionCompensatedException;
import org.sdb.nosql.db.compensation.CounterService;
import org.sdb.nosql.db.connection.MongoConnection;
import org.sdb.nosql.db.performance.ActionRecord;
import org.sdb.nosql.db.performance.ActionTypes;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoException;
import com.mongodb.WriteResult;

public class MongoCompensator extends Mongo {
	
	private AtomicInteger compensations = new AtomicInteger(0);
	
	public MongoCompensator(MongoConnection connection) {
		super(connection);
	}
	
	
	// Reads will be the same a Mongo, however any writes/updates need to go via
	// the
	// Compensation methods
	@Override
	public ActionRecord balanceTransfer(String key1, String key2, int amount,
			int waitMillis) {
		ActionRecord record = new ActionRecord( ActionTypes.BAL_TRAN);	
				
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

			collection.update(query1, set1); // Update
																	
			waitBetweenActions(waitMillis); // Delay
			
			collection.update(query2, set2); // Update
																	
		} catch (MongoException e) {
			
			updateSucceeded = false;
			record.setSuccess(false);// most likely a lock failure!
		}
		
		if (updateSucceeded){
			BasicDBObject set2a = new BasicDBObject();
			set2a.append("$inc", new BasicDBObject().append("tx", 1));
			BasicDBObject set2b = new BasicDBObject();
			set2b.append("$inc", new BasicDBObject().append("tx", 1));		
			
			try {
				collection.update(query1, set2a); // Update
																		
				waitBetweenActions(waitMillis); // Delay
				
				collection.update(query2, set2b); // Update
																		

			} catch (MongoException e) {
				record.setSuccess(false);// most likely a lock failure!
			}
		}

		record.setSuccess(updateSucceeded);

		return record;
		
		
	}

	@Override
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
		
		try{
			for (String key : keys) {
	
				BasicDBObject newDocument = new BasicDBObject();
				newDocument.append("$inc", new BasicDBObject().append("tx", 1));
				BasicDBObject searchQuery = new BasicDBObject().append("name", key);
				collection.update(searchQuery, newDocument);

			}
		} catch (MongoException e) {
			record.setSuccess(false);// most likely a lock failure!
		}
		
		
		return record;
	}
	
	

}
