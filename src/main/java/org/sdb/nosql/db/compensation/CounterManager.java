package org.sdb.nosql.db.compensation;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

import org.jboss.narayana.compensations.api.CompensationManager;
import org.jboss.narayana.compensations.api.TxCompensate;
import org.jboss.narayana.compensations.api.TxConfirm;

import javax.inject.Inject;

import java.net.UnknownHostException;

/**
 * @author paul.robinson@redhat.com 28/06/2014
 */
public class CounterManager {


    @Inject
    private CompensationManager compensationManager;

   
    
    @Inject
    IncrementCounterData incrementCounterData;

    @Inject
    DecrementCounterData decrementCounterData;

    private static ThreadLocal<MongoClient> mongoClients = new ThreadLocal<MongoClient>();

    public CounterManager() {
    	
    }


    @TxCompensate(UndoIncrement.class)
    @TxConfirm(ConfirmIncrement.class)
    public void incrimentCounter(int counter, int amount) {

        //incrementCounterData.setCounter(counter);
       // incrementCounterData.setAmount(amount);

        MongoClient mongoClient = getMongoClient();
        DB database = mongoClient.getDB("test");
        DBCollection accounts = database.getCollection("counters");

        accounts.update(new BasicDBObject("name", String.valueOf(counter)), new BasicDBObject("$inc", new BasicDBObject("value", amount)));

    }

    @TxCompensate(UndoIncrement.class)
    @TxConfirm(ConfirmIncrement.class)
	public void incrimentCounter(String key, int amount) {
        
    	incrementCounterData.setoID(key);
        incrementCounterData.setAmount(amount);
		incrementCounterData.setCounterAndAmount(key,amount);
        MongoClient mongoClient = getMongoClient();
        DB database = mongoClient.getDB("test");
        DBCollection accounts = database.getCollection("counters");
        
        accounts.update(new BasicDBObject("name", key), new BasicDBObject("$inc", new BasicDBObject("value", amount)));

		
	}
    
    @TxCompensate(UndoDecrement.class)
    @TxConfirm(ConfirmDecrement.class)
    public void decrementCounter(int counter, int amount) {

        decrementCounterData.setCounter(counter);
        decrementCounterData.setAmount(amount);

        MongoClient mongoClient = getMongoClient();
        DB database = mongoClient.getDB("test");
        DBCollection accounts = database.getCollection("counters");

        accounts.update(new BasicDBObject("name", String.valueOf(counter)), new BasicDBObject("$inc", new BasicDBObject("value", -1 * amount)));

    }
    
    @TxCompensate(UndoDecrement.class)
    @TxConfirm(ConfirmDecrement.class)
	public void decrementCounter(String key, int amount)  {

        decrementCounterData.setoID(key);
        decrementCounterData.setAmount(amount);
		incrementCounterData.setCounterAndAmount(key,amount);
        MongoClient mongoClient = getMongoClient();
        DB database = mongoClient.getDB("test");
        DBCollection accounts = database.getCollection("counters");

        accounts.update(new BasicDBObject("name", key), new BasicDBObject("$inc", new BasicDBObject("value", -1 * amount)));

    }
    
    public static MongoClient getMongoClient() {
        try {
            if (mongoClients.get() == null) {
                MongoClient mongo = new MongoClient("localhost", 27017);
                mongoClients.set(mongo);
            }

            return mongoClients.get();
        } catch (UnknownHostException e) {
            throw new RuntimeException("Failed to connect to MongoDB", e);
        }
    }






}
