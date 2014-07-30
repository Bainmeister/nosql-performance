package org.sdb.nosql.performance;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

import com.foundationdb.*;
import com.foundationdb.async.Function;
import com.foundationdb.tuple.Tuple;

public class InitializeAndCheckFDB {

	// Generate Predictable Keys
	private static List<String> units = Arrays.asList("0", "1", "2", "3", "4",
			"5", "6", "7", "8", "9");

	private static List<String> indexes = initKeys(3);

	/**
	 * @return list of keys
	 */
	private static List<String> initKeys(int figures) {

		List<String> keyList = new ArrayList<String>();

		for (String level4 : units)
			for (String level3 : units)
				for (String level2 : units)
					for (String level1 : units)
						keyList.add(level4 + level3 + level2 + level1);

		return keyList;
	}

	private static void addBalanceKey(Transaction tr, final String c) {

		try {
			tr.set(Tuple.from("value", c).pack(), encodeInt(0));
		} catch (FDBException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException ie) {
			ie.printStackTrace();
		}

	}

	static int checkBalance() {

		FDB fdb = FDB.selectAPIVersion(200);
		Database db = fdb.open();

		return db.run(new Function<Transaction, Integer>() {
			public Integer apply(Transaction tr) {

				int i = 0;
				try {
					// dry an account to the value of 1000
					// tr.set(Tuple.from("balance", c).pack(), encodeInt(1000));
					for (KeyValue kv : tr.getRange(Tuple.from("value").range())) {

						int temp = decodeInt(kv.getValue());
						i = i + temp;

						// Proof that stuff is happening!
						// if (temp!= 0)
						// System.out.println(temp);
					}

				} catch (FDBException e) {
					e.printStackTrace();
				}

				return i;
			}
		});
	}

	/**
	 * encode and int ready for FDB storage
	 * 
	 * @param value
	 * @return
	 */
	private static byte[] encodeInt(int value) {
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
	private static int decodeInt(byte[] value) {
		if (value.length != 4)
			throw new IllegalArgumentException("Array must be of size 4");
		return ByteBuffer.wrap(value).getInt();
	}

	static void initFDB(final int i) {

		FDB fdb = FDB.selectAPIVersion(200);
		Database db = fdb.open();

		db.run(new Function<Transaction, Void>() {

			public Void apply(Transaction tr) {

				tr.clear(Tuple.from("value").range());
				tr.clear(Tuple.from("log1").range());
				tr.clear(Tuple.from("log2").range());
				tr.clear(Tuple.from("log3").range());

				int added = 0;
				for (String key : indexes) {
					added = added + 1;
					if (added < i)
						addBalanceKey(tr, key);
				}
				return null;
			}
		});
	}

}