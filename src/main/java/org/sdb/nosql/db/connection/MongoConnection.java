package org.sdb.nosql.db.connection;

import java.net.UnknownHostException;
import java.util.List;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;

public class MongoConnection implements DBConnection {

	private MongoClient mongoClient;
	DB db;
	DBCollection collection;
	DBCollection log1;
	DBCollection log2;
	DBCollection log3;
	
	public MongoConnection(){
		connectDB();
	}
	
	public void connectDB() {

		try {
			mongoClient = new MongoClient( "localhost" , 27017 );
			db = mongoClient.getDB("test");
			collection = db.getCollection("counters");
			
			log1 = db.getCollection("log1");
			log2 = db.getCollection("log2");
			log3 = db.getCollection("log3");
			
			
		} catch (UnknownHostException e) {
			System.out.println("Uh-oh connection error!");
			e.printStackTrace();
		}
		
	}

	public void disconnectDB() {
		try{
			mongoClient = null;
			db = null;
			collection = null;
		}catch(MongoException e){
			e.printStackTrace();
		}
	}

	public boolean isConnected() {
		return (db == null || mongoClient == null || collection == null) == true ? false : true ;
	}

	public DB getDb() {
		return db;
	}

	public DBCollection getCollection() {
		return collection;
	}

	public DBCollection getLog1() {
		return log1;
	}
	public DBCollection getLog2() {
		return log2;
	}
	public DBCollection getLog3() {
		return log3;
	}
	
}
