package org.sdb.nosql.db.compensation;

import org.sdb.nosql.db.compensation.javax.RunnerService;
import org.sdb.nosql.db.connection.MongoConnection;
import org.sdb.nosql.db.machine.MongoCompensator;
import org.sdb.nosql.db.performance.ActionRecord;
import org.sdb.nosql.db.worker.WorkerParameters;

import com.mongodb.DBCollection;
import javax.jws.WebMethod;
import javax.jws.WebService;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author paul.robinson@redhat.com 10/07/2014
 */
@WebService(serviceName = "HotelServiceService", portName = "HotelService", name = "HotelService", targetNamespace = "http://www.jboss.org/as/quickstarts/compensationsApi/travel/hotel")
public class RunnerServiceImpl implements RunnerService {

    private Random rand = new Random(System.currentTimeMillis());

    private List<String>availibleKeys = new ArrayList<String>();

    private double compensateProbability;

	private WorkerParameters params;
	
    @WebMethod
    public long doWork(WorkerParameters params, List<String> availibleKeys) {
    	
    	this.compensateProbability = params.COMPENSATE_PROB;
    	this.availibleKeys = availibleKeys;
    	this.params = params;
    	
    	//Connect to the db and get relevant collections
    	MongoConnection connection = new MongoConnection();
		DBCollection collection = connection.getCollection();
		DBCollection log1 = connection.getLog1();
		DBCollection log2 = connection.getLog1();
		DBCollection log3 = connection.getLog1();
		
        long startMillis = System.currentTimeMillis();
        for (int i = 0; i < params.getBatchSize(); i++) {
        	workload();
        }
        long endMillis = System.currentTimeMillis();
    	
        
        connection.disconnectDB();
        return endMillis - startMillis;
    }

	ActionRecord workload() {
    	
		ActionRecord record = new ActionRecord();
    	List<String> keysToUse =new ArrayList<String>();
    	MongoCompensator machine = new MongoCompensator(new MongoConnection());
    	
    	final int transactionSize = params.getMaxTransactionSize() == params.getMinTransactionSize() ? params.getMaxTransactionSize():ThreadLocalRandom.current().nextInt(params.getMaxTransactionSize())+params.getMinTransactionSize(); 
    	for (int i =0; i< transactionSize;i++)
    		keysToUse.add(availibleKeys.get(rand.nextInt(availibleKeys.size())));

    	
    	//Get Random number to assign task
    	final int rand1 = ThreadLocalRandom.current() .nextInt(1000);
    	
    	if (rand1< params.getChanceOfRead()){
    		record = machine.read(keysToUse,params.getMillisBetweenActions());
    		
    	}else if(rand1 < params.getChanceOfInsert()){
        		record = machine.insert(transactionSize, params.getMillisBetweenActions());	
    
    	}else if(rand1 < params.getChanceOfUpdate()){
    		record = machine.update(keysToUse, params.getMillisBetweenActions());	
    	      	
    	}else if (rand1 < params.getChanceOfBalanceTransfer()){
    		record = machine.balanceTransfer(keysToUse.get(0), keysToUse.get(1),10 , params.getMillisBetweenActions());
    	
    	}else if (rand1 < params.getChanceOfLogRead()){
    		record = machine.logRead(params.getMillisBetweenActions());
    		
    	}else if (rand1 < params.getChanceOfLogInsert()){
    		record = machine.logInsert( params.getMillisBetweenActions());
    	}
    	
		return record;
	}

}
