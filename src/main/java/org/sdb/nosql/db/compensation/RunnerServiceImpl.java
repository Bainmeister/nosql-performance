package org.sdb.nosql.db.compensation;

import org.jboss.narayana.compensations.api.TransactionCompensatedException;
import org.sdb.nosql.db.compensation.javax.RunnerService;

import javax.inject.Inject;
import javax.jws.WebMethod;
import javax.jws.WebService;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author paul.robinson@redhat.com 10/07/2014
 */
@WebService(serviceName = "HotelServiceService", portName = "HotelService", name = "HotelService", targetNamespace = "http://www.jboss.org/as/quickstarts/compensationsApi/travel/hotel")
public class RunnerServiceImpl implements RunnerService {


    private Random rand = new Random(System.currentTimeMillis());

    private List<String>availibleKeys = new ArrayList<String>();

    @Inject
    private CounterService counterService;

    private double compensateProbability;

    private AtomicInteger compensations = new AtomicInteger(0);
   
    @WebMethod
    public long balanceTransfer(int loops, List<String> keys, double compensateProbability) {
    	
    	this.compensateProbability = compensateProbability;
    	this.availibleKeys = keys;
    	
    	//DBWorker<Void> worker = new DBWorker<Void>(keys);
    	
        long millis = System.currentTimeMillis();
        for (int i = 0; i < loops; i++) {
        	
        	//Select two Random keys
            int rand1 = rand.nextInt(availibleKeys.size());
            int rand2 = rand.nextInt(availibleKeys.size());
            
            
        	//update them
            try {
            	counterService.updateCounters(    availibleKeys.get(rand1), availibleKeys.get(rand2), 1, compensateProbability);
            } catch (TransactionCompensatedException e) {
                compensations.incrementAndGet();
            }
            
        }
        long timeTaken = (System.currentTimeMillis() - millis);
        
        availibleKeys.clear();
       
        return timeTaken;
    }

}
