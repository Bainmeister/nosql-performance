package org.sdb.nosql.db.machine;

import java.util.List;

import org.sdb.nosql.db.connection.MongoConnection;
import org.sdb.nosql.db.performance.ActionRecord;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoException;
import com.mongodb.WriteResult;

public class TokuMXTransactionalSerializable extends TokuMXTransactional {

	public TokuMXTransactionalSerializable(MongoConnection connection) {
		super(connection);
	}

	//Updates work differently, as we need to lock records manually
	@Override
	public ActionRecord update(List<String> keys, int waitMillis) {
		final ActionRecord record = new ActionRecord();

		db.requestStart();
		try {

			db.requestEnsureConnection();
			try {

				db.command(beginTransaction());

				// Lock the records
				for (String key : keys)
					collection.findOne(new BasicDBObject("name", key));

				// Update the records
				boolean updateSucceeded = true;
				for (String key : keys) {
					WriteResult write = collection.update(new BasicDBObject(
							"name", key), new BasicDBObject("value", 2000));
					waitBetweenActions(waitMillis);
					if (write.getN() == 0)
						updateSucceeded = false;
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

		// Query to Increment balance 2
		BasicDBObject set2 = new BasicDBObject();
		set2.append("$inc", new BasicDBObject().append("value", amount));

		db.requestStart();
		try {
			db.requestEnsureConnection();
			// ***** TRANSACTION****//
			try {

				db.command(beginTransaction());

				// Lock the records by reading them.
				// IMPORTANT: The gap between here an the query allows other
				// updates to potentially change the records.
				// A way around this would be to add a timestamp to the record,
				// only getting records if it hasn't been updated
				// The lack of ability to check last update really makes this
				// feel un-transactional.
				collection.findOne(query1).get("value");
				collection.findOne(query2).get("value");
				// To truely ensure we have the best view of the data we should
				// do a snapshot.

				// Write to the records
				WriteResult write1 = collection.update(query1, set1); // Update
																		// record
																		// 1
				waitBetweenActions(waitMillis); // Delay
				WriteResult write2 = collection.update(query2, set2); // Update
																		// record
																		// 2
				waitBetweenActions(waitMillis); // Delay

				updateSucceeded = (write1.getN() == 0) || (write2.getN() == 0) ? false
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
			db.requestDone();
		}
		return record;
	}

	//Explicitly issue a serializable transaction
	BasicDBObject beginTransaction() {

		// Create beginTransaction object
		BasicDBObject beginTransaction = new BasicDBObject();
		beginTransaction.append("beginTransaction", 1);
		beginTransaction.append("isolation", "serializable");

		return beginTransaction;
	}

}
