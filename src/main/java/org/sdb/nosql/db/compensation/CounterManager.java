package org.sdb.nosql.db.compensation;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;

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

    @Inject
    InsertCounterData insertCounterData;
    
    private static ThreadLocal<MongoClient> mongoClients = new ThreadLocal<MongoClient>();

    public CounterManager() {
    	
    }


    public void incrimentCounter(int counter, int amount, DBCollection col) {
    	incrimentCounter(String.valueOf(counter), amount, col);
    }
    @TxCompensate(UndoIncrement.class)
    @TxConfirm(ConfirmIncrement.class)
	public boolean incrimentCounter(String key, int amount,  DBCollection col) {
        
    	incrementCounterData.setoID(key);
        incrementCounterData.setAmount(amount);
		incrementCounterData.setCounterAndAmount(key,amount);
        
		try{
			col.update(new BasicDBObject("name", key), new BasicDBObject("$inc", new BasicDBObject("value", amount)));
		} catch (MongoException e){
			return false;
		}
		return true;
    }
    

    public void decrementCounter(int counter, int amount, DBCollection col) {
    	decrementCounter(String.valueOf(counter),  amount, col);
    }
    @TxCompensate(UndoDecrement.class)
    @TxConfirm(ConfirmDecrement.class)
	public boolean decrementCounter(String key, int amount, DBCollection col)  {

        decrementCounterData.setoID(key);
        decrementCounterData.setAmount(amount);
		incrementCounterData.setCounterAndAmount(key,amount);
		
		try{
			col.update(new BasicDBObject("name", key), new BasicDBObject("$inc", new BasicDBObject("value", -1 * amount)));
		} catch (MongoException e){
			return false;
		}
		
		return true;
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

    @TxCompensate(UndoInsert.class)
    @TxConfirm(ConfirmInsert.class)
	public boolean insertCounter(String key, int amount, DBCollection col) {
        
		insertCounterData.setoID(key);
		insertCounterData.setAmount(amount);
		insertCounterData.setCounterAndAmount(key,amount);
		
		try{
			col.insert(new BasicDBObject("name", key).append("value", 0).append("tx", 0));
		} catch (MongoException e){
			return false;
		}
		
		return true;
	}

}
