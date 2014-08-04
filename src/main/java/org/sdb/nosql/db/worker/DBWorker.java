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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.sdb.nosql.db.connection.FoundationConnection;
import org.sdb.nosql.db.connection.MongoConnection;
import org.sdb.nosql.db.machine.DBMachine;
import org.sdb.nosql.db.machine.FoundationDB;
import org.sdb.nosql.db.machine.FoundationDBNoRetry;
import org.sdb.nosql.db.machine.Mongo;
import org.sdb.nosql.db.machine.MongoCompensator;
import org.sdb.nosql.db.machine.TokuMX;
import org.sdb.nosql.db.machine.TokuMXTransactional;
import org.sdb.nosql.db.machine.TokuMXTransactionalBestOfBoth;
import org.sdb.nosql.db.machine.TokuMXTransactionalMVCC;
import org.sdb.nosql.db.machine.TokuMXTransactionalSerializable;
import org.sdb.nosql.db.performance.ActionRecord;

/**
 * @author <a href="mailto:s.bain@newcastle.ac.uk">Simon Bain</a>
 *
 * A Worker that interacts with DBMachine to manipulate Databases. 
 * @param <T>
 */
public class DBWorker{

	private long workTimeMillis;
	private long initTimemillis;
	private long finiTimeMillis;
	
	private List<String> contendedRecords;
	private WorkerParameters params;
	
	private DBMachine machine;
	
	//Construct using a list of contended records and the parameters for running the test. 
	public DBWorker(List<String> contendedRecords, WorkerParameters params){
		
		this.contendedRecords = contendedRecords;
		this.params = params;
		
		if (params.isCompensator()){
			this.machine = new MongoCompensator(new MongoConnection());
			
		}else if (params.getDbType() == DBTypes.FOUNDATIONDB){
			this.machine = new FoundationDB(new FoundationConnection());
			
		}else if (params.getDbType() == DBTypes.FOUNDATIONDB_NO_RETRY){
			this.machine = new FoundationDBNoRetry(new FoundationConnection());		
		
		}else if (params.getDbType() == DBTypes.MONGODB){
			this.machine = new Mongo(new MongoConnection());
			
		}else if (params.getDbType() == DBTypes.TOKUMX){
			this.machine = new TokuMX(new MongoConnection());
			
		}else if (params.getDbType() == DBTypes.TOKUMX_TRANS){
			this.machine = new TokuMXTransactional(new MongoConnection());	
			
		}else if (params.getDbType() == DBTypes.TOKUMX_TRANS_MVCC)	{
			this.machine = new TokuMXTransactionalMVCC(new MongoConnection());
			
		}else if (params.getDbType() == DBTypes.TOKUMX_TRANS_SERIALIABLE)	{
			this.machine = new TokuMXTransactionalSerializable(new MongoConnection());
		}else if (params.getDbType() == DBTypes.TOKUMX_TRANS_BoB)	{
			this.machine = new TokuMXTransactionalBestOfBoth(new MongoConnection());
		}

	}


	public Measurement doWork() {
		
		int batchSize = params.getBatchSize();
		Measurement measurement = new Measurement();
			
		ActionRecord record = null;
		
		if (machine == null){
			System.out.println("Something is wrong.  No DBMachine was set");
		}
				
		//Do the work get the measurements
		for (int i = 0 ; i < batchSize;i++){
			
			//////////////RUN THE WORKLOAD///////////////////
			long startTimeMillis = System.currentTimeMillis();
			record = workload();
			long endTimeMillis = System.currentTimeMillis();
			//////////////RUN THE WORKLOAD///////////////////
			
			boolean success = (record.isSuccess())?true:false;
			boolean failed = ( record ==null || !record.isSuccess() || record.isDataFailue() || record.isLockFailure() )? true:false ;
	    	measurement.addToMeasuement(1, success?1:0, failed?1:0, endTimeMillis-startTimeMillis);
	    	
		}
	
    	
		return measurement;
	}
	
    private ActionRecord workload( ) {
    	ActionRecord record = new ActionRecord();
    	
    	final int transactionSize = params.getMaxTransactionSize() == params.getMinTransactionSize() ? params.getMaxTransactionSize():ThreadLocalRandom.current().nextInt(params.getMaxTransactionSize())+params.getMinTransactionSize(); 
    	List<String> keysToUse = getKeysForTransaction(transactionSize); 
    	
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
    		record = machine.logRead(params.getMillisBetweenActions(), params.getLogReadLimit());
    		
    	}else if (rand1 < params.getChanceOfLogInsert()){
    		record = machine.logInsert( params.getMillisBetweenActions());
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
	public long getInitTimeMillis() {
		return initTimemillis;
	}

	/**
	 * @return the finiTimeMillis
	 */
	public long getFiniTimeMillis() {
		return finiTimeMillis;
	}
	
}