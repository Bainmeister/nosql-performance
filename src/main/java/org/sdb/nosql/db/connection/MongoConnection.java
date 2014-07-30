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
	
	public MongoConnection(){
		connectDB();
	}
	
	public void connectDB() {

		try {
			mongoClient = new MongoClient( "localhost" , 27017 );
			db = mongoClient.getDB("test");
			collection = db.getCollection("counters");
			
		} catch (UnknownHostException e) {
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

	public List<DBCollection> getLogCollections() {
		// TODO Auto-generated method stub
		return null;
	}

}
