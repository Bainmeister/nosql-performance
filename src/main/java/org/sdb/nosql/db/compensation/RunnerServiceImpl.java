package org.sdb.nosql.db.compensation;

import org.jboss.narayana.compensations.api.TransactionCompensatedException;
import org.sdb.nosql.db.compensation.javax.RunnerService;
import org.sdb.nosql.db.connection.DBConnection;
import org.sdb.nosql.db.connection.MongoConnection;
import org.sdb.nosql.db.machine.DBMachine;
import org.sdb.nosql.db.performance.ActionRecord;
import org.sdb.nosql.db.worker.WorkerParameters;

import com.mongodb.DBCollection;

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
public class RunnerServiceImpl implements RunnerService, DBMachine {


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
    	
    	MongoConnection conn = new MongoConnection();
    	DBCollection col = conn.getCollection();
    	
    	
        long millis = System.currentTimeMillis();
        for (int i = 0; i < loops; i++) {
        	
        	//Select two Random keys -shouldnt matter if its the same one.
            int rand1 = rand.nextInt(availibleKeys.size());
            int rand2 = rand.nextInt(availibleKeys.size());
            
        	//update them
            try {
            	counterService.updateCounters(    availibleKeys.get(rand1), availibleKeys.get(rand2), 1, compensateProbability, col);
            } catch (TransactionCompensatedException e) {
                compensations.incrementAndGet();
            }
            
        }
        long timeTaken = (System.currentTimeMillis() - millis);
        conn.disconnectDB();
        
        availibleKeys.clear();
       
        return timeTaken;
    }

	@Override
	public long doWork(WorkerParameters params) {
		// This method does what doWork does for a standard DB interaction.
		return 0;
	}

	@Override
	public ActionRecord read(List<String> keys, int waitMillis) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ActionRecord update(List<String> keys, int waitMillis) {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public ActionRecord balanceTransfer(String key1, String key2, int amount, int waitMillis) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ActionRecord logInsert(int waitMillis) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ActionRecord logRead(int waitMillis) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ActionRecord insert(int numberToAdd, int waitMillis) {
		// TODO Auto-generated method stub
		return null;
	}

}
