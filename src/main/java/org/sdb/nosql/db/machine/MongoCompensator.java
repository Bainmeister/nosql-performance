package org.sdb.nosql.db.machine;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javax.inject.Inject;

import org.jboss.narayana.compensations.api.TransactionCompensatedException;
import org.sdb.nosql.db.compensation.CounterService;
import org.sdb.nosql.db.connection.MongoConnection;
import org.sdb.nosql.db.performance.ActionRecord;

public class MongoCompensator extends Mongo {

	private AtomicInteger compensations = new AtomicInteger(0);

	private int compensateProbability = 0;
	
	public MongoCompensator(MongoConnection connection) {
		super(connection);
	}

	@Inject
	private CounterService counterService;
	
	// Reads will be the same a Mongo, however any writes/updates need to go via
	// the
	// Compensation methods
	@Override
	public ActionRecord balanceTransfer(String key1, String key2, int amount,
			int waitMillis) {
		ActionRecord record = new ActionRecord();	
		
		try {
			counterService.updateCounters( key1, key2, amount, compensateProbability, collection, waitMillis);
		} catch (TransactionCompensatedException e) {
			compensations.incrementAndGet();
		}
		
		return record;
	}

	@Override
	public ActionRecord update(List<String> keys, int waitMillis) {
		final ActionRecord record = new ActionRecord();
		try {
			counterService.update(keys, compensateProbability, collection, waitMillis);
		} catch (TransactionCompensatedException e) {
			compensations.incrementAndGet();
		}
		return record;
	}
	
	

}
