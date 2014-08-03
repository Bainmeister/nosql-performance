package org.sdb.nosql.db.machine;

import java.util.List;

import org.sdb.nosql.db.connection.MongoConnection;
import org.sdb.nosql.db.performance.ActionRecord;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoException;
import com.mongodb.WriteResult;

public class TokuMXTransactionalBestOfBoth extends TokuMXTransactional {

	public TokuMXTransactionalBestOfBoth(MongoConnection connection) {
		super(connection);
	}

	// Updates will be Serializable, anything else will be MVCC as standard
	@Override
	BasicDBObject beginTransaction() {
		return beginTransaction("MVCC");
	}
	
	
	//Updates work differently, as we need to lock records manually
	@Override
	public ActionRecord update(List<String> keys, int waitMillis) {
		final ActionRecord record = new ActionRecord();

		db.requestStart();
		try {

			db.requestEnsureConnection();
			try {

				db.command(beginTransaction("serializable"));

				// Lock the records
				for (String key : keys)
					collection.findOne(new BasicDBObject("name", key));

				// Update the records
				boolean updateSucceeded = true;
				for (String key : keys) {
					WriteResult write = collection.update(new BasicDBObject(
							"name", key), new BasicDBObject("value", 2000));
					waitBetweenActions(waitMillis);
					if (write.getN() == 0){
						updateSucceeded = false;
						break; //out of the loop, no point in continuing. 
					}
				}
				// If either write failed, rollback the transaction.
				db.command(updateSucceeded ? commitTransaction()
						: rollbackTransaction());
				record.setSuccess(updateSucceeded);
			} catch (MongoException e) {
				record.setSuccess(false);
				db.command(rollbackTransaction());
			}

		} finally {
			db.requestDone();
		}
		return record;
	}

	//Updates work differently, as we need to lock records manually
	@Override
	public ActionRecord balanceTransfer(String key1, String key2, int amount,
			int waitMillis) {
		final ActionRecord record = new ActionRecord();
		
		boolean updateSucceeded = false;
		record.setSuccess(false);
		if ((key1 == null || key1 == "") || (key2 == null || key2 == "")) {
			System.out.println("2 are keys required for balance transfer");
		}

		// Amount to transfer
		amount = key1 == key2 ? 0 : amount;

		// Setup search querys
		BasicDBObject query1 = new BasicDBObject("name", key1);
		BasicDBObject query2 = new BasicDBObject("name", key2);

		// Query to Decrement balance 1
		BasicDBObject set1 = new BasicDBObject();
		set1.append("$inc", new BasicDBObject().append("value", -amount));
		//set1.append("$inc", new BasicDBObject().append("tx", 1));
		
		// Query to Increment balance 2
		BasicDBObject set2 = new BasicDBObject();
		set2.append("$inc", new BasicDBObject().append("value", amount));
		//set2.append("$inc", new BasicDBObject().append("tx", 1));
		
		db.requestStart();
		try {
			db.requestEnsureConnection();
			// ***** TRANSACTION****//
			try {
				
				db.command(beginTransaction("serializable"));
				// Lock the records
				
				// Lock the records by reading them.
				// IMPORTANT: The gap between here an the query allows other
				// updates to potentially change the records.
				// - The way around this would be to use 2 separate transactions.  First 
				//   an MVCC transaction would take a snapshot of the data.  Then a second 
				//   serializable transaction would lock records and  compare the state to 
				//   the snapshot.  If the comparison was ok, the second transaction would 
				//   do the relevant changes.
				collection.findOne(new BasicDBObject("name", key1));
				collection.findOne(new BasicDBObject("name", key2));
				// To truely ensure we have the best view of the data we should
				// do a snapshot.

				// Write to the records
				WriteResult write1 = collection.update(query1, set1); // Update
																		
				waitBetweenActions(waitMillis); // Delay
				
				WriteResult write2 = collection.update(query2, set2); // Update															
				
				updateSucceeded = (write1 ==null || write2 == null || write1.getN() == 0) || (write2.getN() == 0) ? false
						: true;

				// If either write failed, rollback the transaction.
				db.command(updateSucceeded ? commitTransaction()
						: rollbackTransaction());
				
			} catch (MongoException e) {

				record.setSuccess(false);// most likely a lock failure!
				db.command(rollbackTransaction());
			}
			// ***** TRANSACTION OVER ****//

			record.setSuccess(updateSucceeded);
		} finally {
			// is this catching us out?	
			db.requestDone();
		}
		return record;
	}
	
	
	
	

}
