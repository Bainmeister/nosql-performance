package org.sdb.nosql.db.compensation.javax;

import java.util.List;

import javax.jws.WebService;

import org.sdb.nosql.db.worker.WorkerParameters;



/**
 * @author paul.robinson@redhat.com 10/07/2014
 */
@WebService(name = "HotelService", targetNamespace = "http://www.jboss.org/as/quickstarts/compensationsApi/travel/hotel")
public interface RunnerService {

    public long balanceTransfer(int loops, List<String> keys, double compensateProbability);

	public long doWork(WorkerParameters params);
    
}
