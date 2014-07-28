package org.sdb.nosql.db.compensation;

import org.jboss.narayana.compensations.api.Compensatable;
import org.jboss.narayana.compensations.api.CompensationManager;

import javax.inject.Inject;

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
    public void updateCounters(int counter1, int counter2, int amount, double compensateProbability) {

        counterManager.incrimentCounter(counter2, amount);
        counterManager.decrementCounter(counter1, amount);

        if (rand.nextDouble() <= compensateProbability) {
            compensationManager.setCompensateOnly();
        }
    }
    
    @Compensatable
	public void updateCounters(String string, String string2, int amount,
			double compensateProbability) {
       //System.out.println("Key 1: " +string+"Key 2: " +string2);
    	counterManager.incrimentCounter(string, amount);
        counterManager.decrementCounter(string2, amount);

        if (rand.nextDouble() <= compensateProbability) {
            compensationManager.setCompensateOnly();
        }
		
	}
}
