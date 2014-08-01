package org.sdb.nosql.db.compensation.javax;

import java.util.List;

import javax.jws.WebService;

import org.sdb.nosql.db.performance.ActionRecord;
import org.sdb.nosql.db.worker.WorkerParameters;



/**
 * @author paul.robinson@redhat.com 10/07/2014
 */
@WebService(name = "HotelService", targetNamespace = "http://www.jboss.org/as/quickstarts/compensationsApi/travel/hotel")
public interface RunnerService {

	void setChances(int chanceOfRead, int chanceOfInsert, int chanceOfUpdate,
			int chanceOfBalanceTransfer, int chanceOfLogRead,
			int chanceOfLogInsert);

	void setRemaining(int maxTransactionSize, int minTransactionSize,
			double compensateProbability, int batchSize, int millisBetween);
    
	void setContendedRecords(List<String> contendedRecords);
	
	long doWork();

}
