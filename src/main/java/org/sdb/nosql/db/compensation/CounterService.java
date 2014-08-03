package org.sdb.nosql.db.compensation;

import org.jboss.narayana.compensations.api.Compensatable;
import org.jboss.narayana.compensations.api.CompensationManager;

import com.mongodb.DBCollection;

import javax.inject.Inject;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

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
	public void updateCounters(String string, String string2, int amount,
			double compensateProbability, DBCollection col, int waitMillis) {
    	
    	counterManager.incrimentCounter(string, amount, col );
    	waitBetweenActions(waitMillis);
        counterManager.decrementCounter(string2, amount,col );

        if (rand.nextDouble() <= compensateProbability) {
            compensationManager.setCompensateOnly();
        }
		
	}
    
    @Compensatable
	public void update(List<String> keys, double compensateProbability,
			DBCollection collection, int waitMillis) {
    	
    	for (String key : keys){
    		counterManager.incrimentCounter(key, 10, collection );
    		waitBetweenActions(waitMillis);
    		
            if (rand.nextDouble() <= compensateProbability) {
                compensationManager.setCompensateOnly();
            }
    	}
    	
	}
    
	public void waitBetweenActions(int millis) {
		
		if (ThreadLocalRandom.current().nextInt(2)==1 ){
			try {
				TimeUnit.MILLISECONDS.sleep(millis);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	
	}
}
