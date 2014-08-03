package org.sdb.nosql.db.compensation.javax;


import java.util.List;

import javax.jws.WebService;

import org.sdb.nosql.db.worker.Measurement;

import com.mongodb.DBCollection;

/**
 * @author paul.robinson@redhat.com 10/07/2014
 */
@WebService(name = "HotelService", targetNamespace = "http://www.jboss.org/as/quickstarts/compensationsApi/travel/hotel")
public interface RunnerService{
    
    void setCollections();
    
    void setContendedRecords(List<String> availibleKeys);
    
	void setChances(int chanceOfRead, int chanceOfInsert, int chanceOfUpdate,
			int chanceOfBalanceTransfer, int chanceOfLogRead,
			int chanceOfLogInsert);
	
	void setParams(int maxTransactionSize, int minTransactionSize,
			double compensateProbability, int batchSize, int millisBetween, int logReadLimit);

	long run();
    

    
}