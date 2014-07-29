package org.sdb.nosql.performance;

import java.net.UnknownHostException;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;

public class InitializeAndCheckMongo {

	static void setupMongo(int size) throws UnknownHostException{
		MongoClient mongo = new MongoClient("localhost", 27017);
		DB database = mongo.getDB("test");
	
		database.getCollection("counters").drop();
		DBCollection counters = database.getCollection("counters");		
		
		for (int i=1; i < size+1; i++) {
			counters.insert(new BasicDBObject("name", String.valueOf(i)).append("value", 0).append("tx", 0));
		}
	}
	
	
	static int checkMongo() throws UnknownHostException{
		MongoClient mongo = new MongoClient("localhost", 27017);
		DB database = mongo.getDB("test");
		DBCollection counters = database.getCollection("counters");		
		DBCursor allCounters = counters.find();
		
		int i = 0;
		
		try {
			while(allCounters.hasNext()) {
				i = i + (Integer) allCounters.next().get("value");	
			}
		} finally {
			allCounters.close();
		}
		return i;
	}
	
}