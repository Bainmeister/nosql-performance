/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2013 Red Hat, Inc., and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.sdb.nosql.db.machine;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.sdb.nosql.db.connection.FoundationConnection;
import org.sdb.nosql.db.performance.ActionRecord;
import org.sdb.nosql.db.performance.ActionTypes;

import com.foundationdb.Database;
import com.foundationdb.Transaction;
import com.foundationdb.async.Function;
import com.foundationdb.tuple.Tuple;

/**
 * @author <a href="mailto:s.bain@newcastle.ac.uk">Simon Bain</a>
 * 
 *         A FoundationDB specific implementation of DBMachine. Runs the API as
 *         standard.
 */
public class FoundationDB implements DBMachine {

	Database db;

	public FoundationDB(FoundationConnection connection) {
		db = connection.getDb();
	}

	@SuppressWarnings("unused")
	private FoundationDB() {

	}

	public ActionRecord read(final List<String> keys, final int waitMillis) {

		// FDB API
		return db.run(new Function<Transaction, ActionRecord>() {
			ActionRecord record = new ActionRecord(ActionTypes.READ);
			// * START TRANSACTION *********************/
			public ActionRecord apply(Transaction tr) {

				record.setAttemptsTaken(record.getAttemptsTaken() + 1);

				// For every key in the list do a read in this transaction
				for (String key : keys) {
					decodeInt(tr.get(Tuple.from("value", key).pack()).get());

					waitBetweenActions(waitMillis);

				}

				return record;
			}
			// END TRANSACTION *********************/

		});

	}

	public ActionRecord insert(final int number, final int waitMillis) {

		// Attempts to make a individual name - this may not be too accurate if
		// there are loads of writes, but I'm not too bothered about this.
		final String processNum = System.currentTimeMillis() + "_"
				+ ThreadLocalRandom.current().nextInt(10) + ""
				+ ThreadLocalRandom.current().nextInt(10);

		// FDB API
		return db.run(new Function<Transaction, ActionRecord>() {
			ActionRecord record = new ActionRecord(ActionTypes.INSERT);
			// * START TRANSACTION *********************/
			public ActionRecord apply(Transaction tr) {

				record.setAttemptsTaken(record.getAttemptsTaken() + 1);

				for (int i = 0; i < number; i++) {

					tr.set(Tuple.from("value", processNum + "_" + i).pack(),
							encodeInt(0));
					
					waitBetweenActions(waitMillis);

				}

				return record;
			}
			// END TRANSACTION *********************/

		});
	}

	public ActionRecord update(final List<String> keys, final int waitMillis) {

		// Use the DB API to update a single line.
		return db.run(new Function<Transaction, ActionRecord>() {
			
			ActionRecord record = new ActionRecord(ActionTypes.UPDATE);
			
			// START TRANSACTION *********************/
			public ActionRecord apply(Transaction tr) {

				record.setAttemptsTaken(record.getAttemptsTaken() + 1);

				// For every key in the list do a read in this transaction
				for (String key : keys) {
					tr.set(Tuple.from("value", key).pack(), encodeInt(0));

					waitBetweenActions(waitMillis);
				}

				return record;
			}
			// END TRANSACTION *********************/

		});
	
	}

	public ActionRecord balanceTransfer(final String key1, final String key2,
			final int amount, final int waitMillis) {
		
		return db.run(new Function<Transaction, ActionRecord>() {

			ActionRecord record = new ActionRecord(ActionTypes.BAL_TRAN);

			// START TRANSACTION *********************/
			public ActionRecord apply(Transaction tr) {

				record.setAttemptsTaken(record.getAttemptsTaken() + 1);

				int balanceToSet = key1 == key2 ? 0 : amount;
				
				tr.set(Tuple.from("value", key1).pack(), encodeInt(decodeInt(tr
						.get(Tuple.from("value", key1).pack()).get())
						- balanceToSet));
				
				waitBetweenActions(waitMillis);
				
				tr.set(Tuple.from("value", key2).pack(), encodeInt(decodeInt(tr
						.get(Tuple.from("value", key2).pack()).get())
						+ balanceToSet));

				return record;
			}
			// END TRANSACTION *********************/

		});

	}

	public ActionRecord logRead(final int waitMillis, final int limit) {
		final ActionRecord record = new ActionRecord(ActionTypes.READ_LOG);

		// FDB API
		return db.run(new Function<Transaction, ActionRecord>() {

			// * START TRANSACTION *********************/
			public ActionRecord apply(Transaction tr) {

				record.setAttemptsTaken(record.getAttemptsTaken() + 1);
				
				if (limit <=0 ){
					tr.getRange(Tuple.from("log1").range());
					waitBetweenActions(waitMillis);
					tr.getRange(Tuple.from("log2").range());
					waitBetweenActions(waitMillis);
					tr.getRange(Tuple.from("log3").range());
				}else{
					tr.getRange(Tuple.from("log1").range(),limit);
					waitBetweenActions(waitMillis);
					tr.getRange(Tuple.from("log2").range(),limit);
					waitBetweenActions(waitMillis);
					tr.getRange(Tuple.from("log3").range(),limit);
				}
				return record;
			}
			// END TRANSACTION *********************/

		});
	}

	public ActionRecord logInsert(final int waitMillis) {

		// Attempts to make a individual name - this may not be too accurate if
		// there are loads of writes, but I'm not too bothered about this.
		final String processNum = System.currentTimeMillis() + "_"
				+ ThreadLocalRandom.current().nextInt(10) + ""
				+ ThreadLocalRandom.current().nextInt(10);

		// FDB API
		return db.run(new Function<Transaction, ActionRecord>() {
			ActionRecord record = new ActionRecord(ActionTypes.INSERT_LOG);
			// * START TRANSACTION *********************/
			public ActionRecord apply(Transaction tr) {

				record.setAttemptsTaken(record.getAttemptsTaken() + 1);

				tr.set(Tuple.from("log1", processNum).pack(),
						encodeInt(0));
				waitBetweenActions(waitMillis);
				
				waitBetweenActions(waitMillis);

				tr.set(Tuple.from("log2", processNum).pack(),
						encodeInt(0));
				waitBetweenActions(waitMillis);
				
				waitBetweenActions(waitMillis);

				tr.set(Tuple.from("log3", processNum).pack(),
						encodeInt(0));
				waitBetweenActions(waitMillis);

				return record;
			}
			// END TRANSACTION *********************/

		});
	}

	/**
	 * encode and int ready for FDB storage
	 * 
	 * @param value
	 * @return
	 */
	byte[] encodeInt(int value) {
		byte[] output = new byte[4];
		ByteBuffer.wrap(output).putInt(value);
		return output;
	}

	/**
	 * Decode a byte into an int from FDB storage.
	 * 
	 * @param value
	 * @return
	 */
	int decodeInt(byte[] value) {
		if (value.length != 4)
			throw new IllegalArgumentException("Array must be of size 4");
		return ByteBuffer.wrap(value).getInt();
	}

	public void waitBetweenActions(int millis) {
		try {
			TimeUnit.MILLISECONDS.sleep(millis);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
