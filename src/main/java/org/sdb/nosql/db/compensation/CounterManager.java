package org.sdb.nosql.db.compensation;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

import org.jboss.narayana.compensations.api.CompensationManager;
import org.jboss.narayana.compensations.api.TxCompensate;
import org.jboss.narayana.compensations.api.TxConfirm;
import org.sdb.nosql.db.connection.DBConnection;

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


    public void incrimentCounter(int counter, int amount, DBCollection col) {
    	incrimentCounter(String.valueOf(counter), amount, col);
    }
    @TxCompensate(UndoIncrement.class)
    @TxConfirm(ConfirmIncrement.class)
	public void incrimentCounter(String key, int amount,  DBCollection col) {
        
    	incrementCounterData.setoID(key);
        incrementCounterData.setAmount(amount);
		incrementCounterData.setCounterAndAmount(key,amount);
        
        col.update(new BasicDBObject("name", key), new BasicDBObject("$inc", new BasicDBObject("value", amount)));
	
	}
    

    public void decrementCounter(int counter, int amount, DBCollection col) {
    	decrementCounter(String.valueOf(counter),  amount, col);
    }
    @TxCompensate(UndoDecrement.class)
    @TxConfirm(ConfirmDecrement.class)
	public void decrementCounter(String key, int amount, DBCollection col)  {

        decrementCounterData.setoID(key);
        decrementCounterData.setAmount(amount);
		incrementCounterData.setCounterAndAmount(key,amount);

        col.update(new BasicDBObject("name", key), new BasicDBObject("$inc", new BasicDBObject("value", -1 * amount)));

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
