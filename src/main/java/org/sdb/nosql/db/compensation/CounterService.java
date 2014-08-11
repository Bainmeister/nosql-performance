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
    	
    	boolean success1 = counterManager.incrimentCounter(string, amount, col );
    	waitBetweenActions(waitMillis);
    	boolean success2 = counterManager.decrementCounter(string2, amount,col );

    	boolean success = true; 
    			
        if (!success1 || !success2)
        	success=false;
        
        if (!success) {
           compensationManager.setCompensateOnly();
        }
		
	}
    
    @Compensatable
	public void update(List<String> keys, double success,
			DBCollection collection, int waitMillis) {
    	
    	boolean success3 = true;
    	
    	for (String key : keys){
    		if(! counterManager.incrimentCounter(key, 10, collection ))
    			success3 = false;
    		
    		waitBetweenActions(waitMillis);
    		
            if (!success3) {
               compensationManager.setCompensateOnly();
            }
    	}
    	
	}
    
    @Compensatable
	public void insert(List<String> keys, int amount,
			double compensateProbability, DBCollection col, int waitMillis) {
    	
    	
    	for (String key : keys){
        	boolean success1 = counterManager.insertCounter(key, amount, col );
        	waitBetweenActions(waitMillis);
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
