package org.sdb.nosql.db.compensation.javax;


import javax.jws.WebService;

import com.mongodb.DBCollection;

/**
 * @author paul.robinson@redhat.com 10/07/2014
 */
@WebService(name = "HotelService", targetNamespace = "http://www.jboss.org/as/quickstarts/compensationsApi/travel/hotel")
public interface RunnerService{

	public void setCollections();
	
    public int run(int loops, int counters, double compensateProbability);

}