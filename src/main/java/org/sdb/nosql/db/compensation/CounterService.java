package org.sdb.nosql.db.compensation;

import org.jboss.narayana.compensations.api.Compensatable;
import org.jboss.narayana.compensations.api.CompensationManager;
import org.sdb.nosql.db.connection.DBConnection;

import com.mongodb.DBCollection;

import javax.inject.Inject;

import java.util.List;
import java.util.Random;

/**
 * This service is responsible for transferring money from one account to another.
 *
 * @author paul.robinson@redhat.com 09/01/2014
 */
public class CounterService {

    private static Random rand = new Random(System.currentTimeMillis());

    @Inject
    CounterManager counterManager;

    @Inject
    CompensationManager compensationManager;

    @Compensatable
    public void updateCounters(int counter1, int counter2, int amount, double compensateProbability, DBCollection col) {

        counterManager.incrimentCounter(counter2, amount,col);
        counterManager.decrementCounter(counter1, amount,col);

        if (rand.nextDouble() <= compensateProbability) {
            compensationManager.setCompensateOnly();
        }
    }
    
    @Compensatable
	public void updateCounters(String string, String string2, int amount,
			double compensateProbability, DBCollection col) {
       
    	counterManager.incrimentCounter(string, amount, col );
        counterManager.decrementCounter(string2, amount,col );

        if (rand.nextDouble() <= compensateProbability) {
            compensationManager.setCompensateOnly();
        }
		
	}
    
    @Compensatable
	public void update(List<String> keys, int compensateProbability,
			DBCollection collection) {
		
    	for (String key : keys){
    		counterManager.incrimentCounter(key, 1, collection );
    		
            if (rand.nextDouble() <= compensateProbability) {
                compensationManager.setCompensateOnly();
            }
    	}
    	
	}
}
