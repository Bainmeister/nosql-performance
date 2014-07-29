/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2013 Red Hat, Inc., and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.sdb.nosql.db.worker;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import javax.xml.namespace.QName;
import javax.xml.ws.Service;

import io.narayana.perf.Result;
import io.narayana.perf.Worker;

import org.sdb.nosql.db.compensation.javax.RunnerService;
import org.sdb.nosql.db.connection.FoundationConnection;
import org.sdb.nosql.db.connection.MongoConnection;
import org.sdb.nosql.db.keys.generation.KeyGen;
import org.sdb.nosql.db.machine.DBMachine;
import org.sdb.nosql.db.machine.FoundationDB;
import org.sdb.nosql.db.machine.TokuMX;
import org.sdb.nosql.db.machine.TokuMXOptimist;
import org.sdb.nosql.db.machine.TokuMXPessimist;
import org.sdb.nosql.db.performance.ActionRecord;

/**
 * @author <a href="mailto:s.bain@newcastle.ac.uk">Simon Bain</a>
 *
 * A Worker that interacts with DBMachine to manipulate Databases. 
 * @param <T>
 */
//@WebService(serviceName = "HotelServiceService", portName = "HotelService", name = "HotelService", targetNamespace = "http://www.jboss.org/as/quickstarts/compensationsApi/travel/hotel")
public class DBWorker<T> implements Worker<T>{

	private long workTimeMillis;
	private long initTimemillis;
	private long finiTimeMillis;
	
	private List<String> contendedRecords;
	private WorkerParameters params;
	
	DBMachine machine;
	
	//Construct
	public DBWorker(List<String> contendedRecords, WorkerParameters params){
		this.contendedRecords = contendedRecords;
		this.params = params;
	}
			
	
	@Override
	public T doWork(T context, int batchSize, Result<T> measurement) {
		
		long timetaken = 0;
		
		//Call the doWork 
		if (params.isCompensator()){
			
			//TODO add and actionRecord to pass back rather than workTimeMillis
	        RunnerService runnerService = createWebServiceClient();
	        workTimeMillis = runnerService.doWork(params);
	        workTimeMillis  = timetaken;
	        return null;
		}
		
		//now connect to the required db and setup relevant database machine.
		//Call the doWork 
		if (!params.isCompensator()){
				
			//Set up the relevant Database Machine with a connection to the DB
			if (params.getDbType() == DBTypes.FOUNDATIONDB){
				machine = new FoundationDB(new FoundationConnection());
				
			}else if (params.getDbType() == DBTypes.TOKUMX){
				machine = new TokuMX(new MongoConnection());
				
			}else if (params.getDbType() == DBTypes.TOKUMX_ACID_OC)	{
				machine = new TokuMXOptimist(new MongoConnection());
				
			}else if (params.getDbType() == DBTypes.TOKUMX_ACID_PC)	{
				machine = new TokuMXPessimist(new MongoConnection());
			}
				
		}
		
		//Carry out a batch of work
		for (int i = 0 ; i < batchSize;i++){
			ActionRecord record = workload();
		}
		
		//System.out.println("time taken: " + timetaken);
		workTimeMillis = System.currentTimeMillis();
		return null;
	}
	
    private ActionRecord workload() {
    	ActionRecord record = new ActionRecord();
    	
    	final int transactionSize = params.getMaxTransactionSize() == params.getMinTransactionSize() ? params.getMaxTransactionSize():ThreadLocalRandom.current().nextInt(params.getMaxTransactionSize())+params.getMinTransactionSize(); 
    	List<String> keysToUse = getKeysForTransaction(transactionSize); 
    	
    	////System.out.println("key1:" keysToUse.get(0));
    	//System.out.println(keysToUse.get(1));
    	//Get Random number to assign task
    	final int rand1 = ThreadLocalRandom.current() .nextInt(1000);
    	if (rand1< params.getChanceOfRead()){
    		//Reader
    		record = machine.read(keysToUse,params.getMillisBetweenActions());
    		
    	}else if(rand1 < params.getChanceOfWrite()){
    		//Writer
    		record = machine.update(keysToUse, params.getMillisBetweenActions());
    		
    	}else if(rand1 < params.getChanceOfReadModifyWrite()){
    		//Reader + Writer
    		record = machine.readModifyWrite(keysToUse, params.getMillisBetweenActions());
    		
    	}else if (rand1 < params.getChanceOfBalanceTransfer()){
    		record = machine.balanceTransfer(keysToUse.get(0), keysToUse.get(1), params.getMillisBetweenActions());
    	
    	}else if (rand1 < params.getChanceOfIncrementalUpdate()){
    		//record = machine.incrementalUpdate(keysToUse, params.getMillisBetweenActions());
    		
    	}
		return record;
	}

    private List<String> getKeysForTransaction(int numberToGet){

    	List<String>  keys= new ArrayList<String> ();
    	List<Integer>  used= new ArrayList<Integer> ();
    	
    	//If the transaction is too large, reduce it to a size we can hanle.
    	if(numberToGet > contendedRecords.size()){
    		System.out.println("Warning! Transaction size too large - reducing to "+contendedRecords.size());
    		numberToGet =  contendedRecords.size();
    	}
    	
    	while(keys.size() < numberToGet){
    		int recordAt = ThreadLocalRandom.current().nextInt(contendedRecords.size()); 
    		
    		if(!used.contains(recordAt)){
    			used.add(recordAt);
    			keys.add(contendedRecords.get(recordAt));
    		}
    	}
    	
    	return keys;
    }
    
    
	private static RunnerService createWebServiceClient() {

        try {
            URL wsdlLocation = new URL("http://localhost:8080/test/HotelServiceService?wsdl");
            QName serviceName = new QName("http://www.jboss.org/as/quickstarts/compensationsApi/travel/hotel",
                    "HotelServiceService");
            QName portName = new QName("http://www.jboss.org/as/quickstarts/compensationsApi/travel/hotel",
                    "HotelService");

            Service service = Service.create(wsdlLocation, serviceName);
            return service.getPort(portName, RunnerService.class);
        } catch (MalformedURLException e) {
            throw new RuntimeException("Error creating Web Service client", e);
        }
    }

	public void init() {
		initTimemillis = System.currentTimeMillis();
		
	}

	public void fini() {
		finiTimeMillis = System.currentTimeMillis();
		
	}

	/**
	 * @return the workTimeMillis
	 */
	public long getWorkTimeMillis() {
		return workTimeMillis;
	}

	/**
	 * @return the initTimemillis
	 */
	public long getInitTimemillis() {
		return initTimemillis;
	}

	/**
	 * @return the finiTimeMillis
	 */
	public long getFiniTimeMillis() {
		return finiTimeMillis;
	}
	
}