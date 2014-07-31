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

	@WebMethod
	public long doWork(List<String> availibleKeys, int chanceOfRead,
			int chanceOfInsert, int chanceOfUpdate,
			int chanceOfBalanceTransfer, int chanceOfLogRead,
			int chanceOfLogInsert, int maxTransactionSize, 
			int minTransactionSize, double compensateProbability, 
			int batchSize, int millisBetween) {

		// Connect to the db and get relevant collections
		MongoConnection connection = new MongoConnection();
		DBCollection collection = connection.getCollection();
		DBCollection log1 = connection.getLog1();
		DBCollection log2 = connection.getLog1();
		DBCollection log3 = connection.getLog1();

		long startMillis = System.currentTimeMillis();
		for (int i = 0; i < batchSize; i++) {
			workload(availibleKeys, chanceOfRead, chanceOfInsert, chanceOfUpdate,
					chanceOfBalanceTransfer, chanceOfLogRead, chanceOfLogInsert,
					maxTransactionSize, minTransactionSize, compensateProbability, millisBetween);
		}
		long endMillis = System.currentTimeMillis();

		// connection.disconnectDB();
		return endMillis - startMillis;
	}

	ActionRecord workload(List<String> availibleKeys, int chanceOfRead,
							int chanceOfInsert, int chanceOfUpdate,
							int chanceOfBalanceTransfer, int chanceOfLogRead,
							int chanceOfLogInsert, int maxTransactionSize, 
							int minTransactionSize,	double compensateProbability,
							int millisBetween) {

		ActionRecord record = new ActionRecord();

		List<String> keysToUse = new ArrayList<String>();
		MongoCompensator machine = new MongoCompensator(new MongoConnection());

		final int transactionSize = maxTransactionSize == minTransactionSize ? maxTransactionSize
				: ThreadLocalRandom.current().nextInt(maxTransactionSize) + minTransactionSize;
		
		for (int i = 0; i < transactionSize; i++)
			keysToUse
					.add(availibleKeys.get(rand.nextInt(availibleKeys.size())));

		// Get Random number to assign task 
		final int rand1 = ThreadLocalRandom.current().nextInt(1000);

		if (rand1 < chanceOfRead) {
			record = machine.read(keysToUse, millisBetween);

		} else if (rand1 < chanceOfInsert) {
			record = machine.insert(transactionSize, millisBetween);

		} else if (rand1 < chanceOfUpdate) {
			record = machine
					.update(keysToUse, millisBetween);

		} else if (rand1 < chanceOfBalanceTransfer) {
			record = machine.balanceTransfer(keysToUse.get(0),
					keysToUse.get(1), 10, millisBetween);

		} else if (rand1 < chanceOfLogRead) {
			record = machine.logRead(millisBetween);

		} else if (rand1 < chanceOfLogInsert) {
			record = machine.logInsert(millisBetween);
		}

		return record;
	}

}
