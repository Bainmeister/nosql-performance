package org.sdb.nosql.db.keys.generation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.bson.BasicBSONObject;
import org.sdb.nosql.db.connection.FoundationConnection;
import org.sdb.nosql.db.connection.MongoConnection;
import org.sdb.nosql.db.keys.storage.KeyStore;

import com.foundationdb.Database;
import com.foundationdb.FDB;
import com.foundationdb.KeyValue;
import com.foundationdb.Transaction;
import com.foundationdb.async.Function;
import com.foundationdb.tuple.Tuple;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

public class KeyGen implements KeyGeneratorIF {

	private boolean isMongo = false;
	private boolean isFDB = false;

	private Database fdbDB;
	private FDB FDB;
	private DB mongoDB;
	private DBCollection mongoCollection;

	@SuppressWarnings("unused")
	private KeyGen() {
		// ONLY ALLOW CONSTRUCTION WITH CONNECTION
	}

	public KeyGen(FoundationConnection conn) {
		if (isMongo)
			return;

		isFDB = true;
		FDB = conn.getFdb();
		fdbDB = conn.getDb();
	}

	public KeyGen(MongoConnection conn) {
		if (isFDB)
			return;

		isMongo = true;
		mongoDB = conn.getDb();
		mongoCollection = conn.getCollection();
	}

	public List<String> getKeys(int number) {

		if (isMongo)
			return getMongoKeys(number);

		if (isFDB)
			return getFDBKeys(number);

		// Shouldn't get this far
		return null;
	}

	public List<String> getRandomKeys(int number) {

		if (isMongo)
			return getMongoRandomKeys(number);

		if (isFDB)
			return getFDBRandomKeys(number);

		// Shouldn't get this far
		return null;
	}

	List<String> getMongoKeys(int number) {
		List<String> ks = new ArrayList<String>();

		if (mongoCollection == null)
			return null;

		long collectionCount = mongoCollection.getCount();

		if (collectionCount >= number) {

			DBCursor cursor = mongoCollection.find().limit(number + 1);

			try {
				while (cursor.hasNext()) {
					ks.add((String) cursor.next().get("name"));
				}
			} finally {
				cursor.close();
			}

		} else {
			System.out.println("check settings...");
		}

		return ks;
	}

	List<String> getMongoRandomKeys(int number) {
		List<String> ks = new ArrayList<String>();

		if (mongoCollection == null)
			return null;

		long collectionCount = mongoCollection.getCount();

		if (collectionCount >= number) {

			// Get Random documents from the DB and store them in the key map.
			while (ks.size() < number) {
				final int rand = ThreadLocalRandom.current().nextInt(
						(int) collectionCount);
				BasicDBObject query = new BasicDBObject();
				BasicDBObject field = new BasicDBObject();
				field.put("_id", 1);
				ks.add((((BasicBSONObject) (mongoCollection.find(query, field)
						.limit(-1).skip(rand)).next()).getString("_id")));
			}

		} else {
			System.out.println("check settings...");
		}

		return ks;
	}

	List<String> getFDBKeys(final int number) {

		return fdbDB.run(new Function<Transaction, List<String>>() {
	    	
	    	/** START TRANSACTION *********************/
		    public List<String> apply(Transaction tr) {
		    
	    		List<String> keys = new ArrayList<String>();	

	    		
	    		for(KeyValue kv: tr.getRange(Tuple.from("value").range(),number)){
	    			keys.add(Tuple.fromBytes(kv.getKey()).getString(1));
	    		}
	
	    		return keys;
		   
		    }
		    /** END TRANSACTION   *********************/
	    
	    });	
		
	}

	List<String> getFDBRandomKeys(int number) {
		List<String> ks = new ArrayList<String>();
		// TODO URGERNT!
		if (number > 9999)
			return null;

		ks.add("0000");
		ks.add("0001");
		return ks;
	}
}
