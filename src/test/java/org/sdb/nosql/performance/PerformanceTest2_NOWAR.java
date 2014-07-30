/*
 * JBoss, Home of Professional Open Source
 * Copyright 2006, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags.
 * See the copyright.txt in the distribution for a full listing
 * of individual contributors.
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 *
 * (C) 2005-2006,
 * @author JBoss Inc.
 */
package org.sdb.nosql.performance;

import java.util.List;

import io.narayana.perf.Result;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.sdb.nosql.db.connection.FoundationConnection;
import org.sdb.nosql.db.connection.MongoConnection;
import org.sdb.nosql.db.keys.generation.KeyGen;
import org.sdb.nosql.db.worker.DBTypes;
import org.sdb.nosql.db.worker.DBWorker;
import org.sdb.nosql.db.worker.WorkerParameters;

public class PerformanceTest2_NOWAR {	
	
	//Test parameters
	private WorkerParameters params = new WorkerParameters(		DBTypes.TOKUMX_TRANS_SERIALIABLE,  	//DB Type
																false, 				//Compensatory?
																10, 				//Thread Count
																1000, 				//Number of Calls
																10, 				//Batch Size
																2					//Contended Records
															);
	private void setTestParams(){
		
		params.setChanceOfBalanceTransfer(1000);
		params.setChanceOfRead(0);
		params.setChanceOfUpdate(0);
		
		params.setMaxTransactionSize(2);
		params.setMinTransactionSize(2);
		params.setMillisBetweenActions(0);
	}
	
	
	/**
	 * Setup the initial test data. Give both counters 'A' and 'B' £1000
	 *
	 * @throws Exception
	 */
	@Before
	public void resetAccountData() throws Exception {

		int dbType = params.getDbType(); 
		
		if (dbType == DBTypes.FOUNDATIONDB || dbType == DBTypes.FOUNDATIONDB_BLOCK_NO_RETRY || dbType ==DBTypes.FOUNDATIONDB_NO_RETRY){
			InitializeAndCheckFDB.initFDB(params.getContendedRecords());
		}else {
			InitializeAndCheckMongo.setupMongo(params.getContendedRecords());
		}
			
	}


	
	@Test
	public void perf() throws Exception {
		System.out.println("***************************");
		System.out.println("PERFORMANCE TEST");
		System.out.println("Threads:    "+ params.getThreadCount());
		System.out.println("Batch size: "+ params.getBatchSize());
		System.out.println("Calls:      "+ params.getNumberOfCalls());
		//System.out.println("***************************");
		
		//Pre-test
		setTestParams();
		List<String> contendedKeys = null;
		
		//1) Connect to the DB and grab some keys
		int dbType = params.getDbType();
		if (dbType == DBTypes.FOUNDATIONDB || dbType == DBTypes.FOUNDATIONDB_BLOCK_NO_RETRY || dbType ==DBTypes.FOUNDATIONDB_NO_RETRY){
			contendedKeys = new KeyGen(new FoundationConnection()).getKeys(params.getContendedRecords());
		}else {
			contendedKeys = new KeyGen(new MongoConnection()).getKeys(params.getContendedRecords());
		}
		
		
		//2) Setup the template worker using the contended keys + other parameters
		DBWorker<Void> workerTemplate = new DBWorker<Void>(contendedKeys,params);

		//3) Run the test with a warm up cycle of 100
		Result<Void> measurement = new Result<Void>(params.getThreadCount(), 
													params.getNumberOfCalls(), 
													params.getBatchSize())
													.measure(workerTemplate, workerTemplate, 100);
		
		//keys.addAll(cursor);
		System.out.println("***************************");
		if (params.isCompensator() == true)
			System.out.println("COMPENSATION BASED");
		System.out.println("Time taken:      "  + measurement.getTotalMillis());
		System.out.println("Batch size:      "  + measurement.getBatchSize());
		System.out.println("Thread count:    "  + measurement.getThreadCount());
		System.out.println("Number of calls: "  + measurement.getNumberOfCalls());
		System.out.println("Number of Failures: "  + measurement.getErrorCount());
		System.out.println("***************************");
	}
	
	@After
	public void accountCheck() throws Exception {

		int i = 0;
		int dbType = params.getDbType(); 
		
		if (dbType == DBTypes.FOUNDATIONDB || dbType == DBTypes.FOUNDATIONDB_BLOCK_NO_RETRY || dbType ==DBTypes.FOUNDATIONDB_NO_RETRY){
			i = InitializeAndCheckFDB.checkBalance();
		}else {
			i = InitializeAndCheckMongo.checkMongo();
		}
		
		System.out.println("Variance= "+ i);
		System.out.println("***************************");
	}
}
