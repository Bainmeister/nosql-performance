package org.sdb.nosql.db.machine;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.sdb.nosql.db.connection.FoundationConnection;
import org.sdb.nosql.db.performance.ActionRecord;

import com.foundationdb.FDBException;
import com.foundationdb.Transaction;
import com.foundationdb.tuple.Tuple;

public class FoundationDBNoRetry extends FoundationDB {

	public FoundationDBNoRetry(FoundationConnection connection) {
		super(connection);
		// TODO Auto-generated constructor stub
	}

	public ActionRecord read(List<String> keys, int waitMillis) {

		final ActionRecord record = new ActionRecord();

		Transaction tr1 = db.createTransaction();
		record.setSuccess(true);

		// Do it via blocking (Map)
		try {

			for (String key : keys) {
				decodeInt(tr1.get(Tuple.from("value", key).pack()).get());
			}

			// Commit the transaction!
			tr1.commit().get();
			record.setAttemptsTaken(record.getAttemptsTaken() + 1);

		} catch (FDBException e) {

			// Fail!
			record.setSuccess(false);

		} catch (IllegalArgumentException e) {
			System.out.println("Illegal Argument");
			record.setSuccess(false);
		}

		return record;
	}

	public ActionRecord insert(int number, int waitMillis) {

		final ActionRecord record = new ActionRecord();

		final String processNum = System.currentTimeMillis() + "_"
				+ ThreadLocalRandom.current().nextInt(10) + ""
				+ ThreadLocalRandom.current().nextInt(10);

		Transaction tr1 = db.createTransaction();
		record.setSuccess(true);

		// Do it via blocking (Map)
		try {

			for (int i = 0; i < number; i++) {

				tr1.set(Tuple.from("value", processNum + "_" + i).pack(),
						encodeInt(0));
				waitBetweenActions(waitMillis);

			}

			// Commit the transaction!
			tr1.commit().get();
			record.setAttemptsTaken(record.getAttemptsTaken() + 1);

		} catch (FDBException e) {

			// Fail!
			record.setSuccess(false);

		} catch (IllegalArgumentException e) {
			System.out.println("FDBNoRetry Illegal Argument");
			record.setSuccess(false);
		}

		return record;
	}
	
	public ActionRecord update(List<String> keys, int waitMillis) {

		final ActionRecord record = new ActionRecord();

		Transaction tr1 = db.createTransaction();
		record.setSuccess(true);

		// Do it via blocking (Map)
		try {

			// For every key in the list do a read in this transaction
			for (String key : keys) {
				tr1.set(Tuple.from("value", key).pack(), encodeInt(0));

				waitBetweenActions(waitMillis);
			}

			// Commit the transaction!
			tr1.commit().get();
			record.setAttemptsTaken(record.getAttemptsTaken() + 1);

		} catch (FDBException e) {

			// Fail!
			record.setSuccess(false);

		} catch (IllegalArgumentException e) {
			System.out.println("FDBNoRetry Illegal Argument");
			record.setSuccess(false);
		}

		return record;
	}

	public ActionRecord balanceTransfer(final String key1, final String key2,
			final int amount, final int waitMillis) {

		final ActionRecord record = new ActionRecord();

		Transaction tr1 = db.createTransaction();
		record.setSuccess(true);

		// Do it via blocking (Map)
		try {

			int balanceToSet = key1 == key2 ? 0 : amount;

			tr1.set(Tuple.from("value", key1).pack(), encodeInt(decodeInt(tr1
					.get(Tuple.from("value", key1).pack()).get())
					- balanceToSet));
			
			waitBetweenActions(waitMillis);
			
			tr1.set(Tuple.from("value", key2).pack(), encodeInt(decodeInt(tr1
					.get(Tuple.from("value", key2).pack()).get())
					+ balanceToSet));

			// Commit the transaction!
			tr1.commit().get();
			record.setAttemptsTaken(record.getAttemptsTaken() + 1);

		} catch (FDBException e) {

			// Fail!
			record.setSuccess(false);

		} catch (IllegalArgumentException e) {
			System.out.println("FDBNoRetry Illegal Argument");
			record.setSuccess(false);
		}

		return record;
	}

	public ActionRecord logRead(int waitMillis) {

		final ActionRecord record = new ActionRecord();

		Transaction tr1 = db.createTransaction();
		record.setSuccess(true);

		// Do it via blocking (Map)
		try {

			tr1.getRange(Tuple.from("log1").range(),1000);
			tr1.getRange(Tuple.from("log2").range(),1000);
			tr1.getRange(Tuple.from("log3").range(),1000);

			// Commit the transaction!
			tr1.commit().get();
			record.setAttemptsTaken(record.getAttemptsTaken() + 1);

		} catch (FDBException e) {

			// Fail!
			record.setSuccess(false);

		} catch (IllegalArgumentException e) {
			System.out.println("Illegal Argument");
			record.setSuccess(false);
		}

		return record;
	}

	public ActionRecord logInsert(int waitMillis) {

		final ActionRecord record = new ActionRecord();

		final String processNum = System.currentTimeMillis() + "_"
				+ ThreadLocalRandom.current().nextInt(10) + ""
				+ ThreadLocalRandom.current().nextInt(10);

		Transaction tr1 = db.createTransaction();
		record.setSuccess(true);

		// Do it via blocking (Map)
		try {
			
			tr1.set(Tuple.from("log1", processNum).pack(),
					encodeInt(0));
			waitBetweenActions(waitMillis);

			tr1.set(Tuple.from("log2", processNum).pack(),
					encodeInt(0));
			waitBetweenActions(waitMillis);

			tr1.set(Tuple.from("log3", processNum).pack(),
					encodeInt(0));
			waitBetweenActions(waitMillis);
			
			// Commit the transaction!
			tr1.commit().get();
			record.setAttemptsTaken(record.getAttemptsTaken() + 1);

		} catch (FDBException e) {

			// Fail!
			record.setSuccess(false);

		} catch (IllegalArgumentException e) {
			System.out.println("FDBNoRetry Illegal Argument");
			record.setSuccess(false);
		}

		return record;
	}
}
