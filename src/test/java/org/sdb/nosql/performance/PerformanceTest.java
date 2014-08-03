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

///import io.narayana.perf.WorkerWorkload;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.KeyStore;
import java.util.LinkedList;
import java.util.List;

import javax.xml.namespace.QName;
import javax.xml.ws.Service;

import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ArchivePaths;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.EmptyAsset;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.resolver.api.maven.Maven;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sdb.nosql.db.compensation.CounterService;
import org.sdb.nosql.db.compensation.javax.RunnerService;
import org.sdb.nosql.db.connection.DBConnection;
import org.sdb.nosql.db.connection.FoundationConnection;
import org.sdb.nosql.db.connection.MongoConnection;
import org.sdb.nosql.db.keys.generation.KeyGen;
import org.sdb.nosql.db.machine.DBMachine;
import org.sdb.nosql.db.performance.ActionRecord;
import org.sdb.nosql.db.worker.DBTypes;
import org.sdb.nosql.db.worker.DBWorker;
import org.sdb.nosql.db.worker.Measurement;
import org.sdb.nosql.db.worker.WorkerParameters;

import com.foundationdb.Database;

@RunWith(Arquillian.class)
public class PerformanceTest {
	
	
	private boolean isCompensator = false;
	private int dbType = DBTypes.TOKUMX;
	private int threadCount = 150;
	private int batchSize = 50;
	private int contendedRecordNum = 2;

	long runTime = 1000;
	
	private void setTestParams() {

		params.setChanceOfRead(1);
		params.setChanceOfInsert(999);
		params.setChanceOfUpdate(0);
		params.setChanceOfBalanceTransfer(0);
		params.setChanceOfLogRead(0);
		params.setChanceOfLogInsert(0);

		params.setMaxTransactionSize(2);
		params.setMinTransactionSize(2);
		params.setMillisBetweenActions(0);
	
		params.setLogReadLimit(1000);
	}

	// Test parameters
	private WorkerParameters params = 
			new WorkerParameters(dbType, // DBType
								isCompensator, // Compensatory?
								threadCount, // Thread Count
								batchSize, // Batch Size
								2 // Contended Records
	);
	
	private List<String> contendedKeys = null;
	
	@Deployment
	public static WebArchive createTestArchive() {

		//Use 'Shrinkwrap Resolver' to include the mongodb java driver in the deployment
		File lib = Maven.resolver().loadPomFromFile("pom.xml").resolve("org.mongodb:mongo-java-driver:2.10.1").withoutTransitivity().asSingleFile();

		WebArchive archive = ShrinkWrap.create(WebArchive.class, "test.war")
				.addPackages(true, CounterService.class.getPackage().getName(),
								DBWorker.class.getPackage().getName(),
								ActionRecord.class.getPackage().getName(),
								DBConnection.class.getPackage().getName(),
								KeyGen.class.getPackage().getName(),
								KeyStore.class.getPackage().getName(),
								DBMachine.class.getPackage().getName(),
								Database.class.getPackage().getName(),
								InitializeAndCheckMongo.class.getPackage().getName())
				.addAsManifestResource("services/javax.enterprise.inject.spi.Extension")
				.addAsWebInfResource(EmptyAsset.INSTANCE, "beans.xml")
				.addAsLibraries(lib);

		archive.delete(ArchivePaths.create("META-INF/MANIFEST.MF"));

		final String ManifestMF = "Manifest-Version: 1.0\n"
				+ "Dependencies: org.jboss.narayana.txframework\n";
		archive.setManifest(new StringAsset(ManifestMF));

		return archive;
	}
	
	/**
	 * Setup the initial test data. Give both counters 'A' and 'B' £1000
	 * 
	 * @throws Exception
	 */
	@Before
	public void resetAccountData() throws Exception {

		int dbType = params.getDbType();

		if (dbType == DBTypes.FOUNDATIONDB
				|| dbType == DBTypes.FOUNDATIONDB_NO_RETRY) {
			InitializeAndCheckFDB.initFDB(params.getContendedRecords());
		} else {
			InitializeAndCheckMongo.setupMongo(params.getContendedRecords());
		}

	}

	@Test
	public void perf() throws Exception {
		System.out.println("***************************");
		System.out.println("PERFORMANCE TEST");
		System.out.println("Threads:    " + params.getThreadCount());
		System.out.println("Batch size: " + params.getBatchSize());
		System.out.println("***************************");

		// Pre-test
		setTestParams();

		// 1) Connect to the DB and grab some keys
		int dbType = params.getDbType();
		if (dbType == DBTypes.FOUNDATIONDB
				|| dbType == DBTypes.FOUNDATIONDB_NO_RETRY) {
			contendedKeys = new KeyGen(new FoundationConnection())
					.getKeys(params.getContendedRecords());
		} else {
			contendedKeys = new KeyGen(new MongoConnection()).getKeys(params
					.getContendedRecords());
		}

	
		//Setup some threads
		LinkedList<ClientThread> threads = new LinkedList<ClientThread>();
		for (int i = 0; i < params.getThreadCount(); i++) {
			ClientThread clientThread = new ClientThread(contendedKeys,params,runTime);
			threads.add(clientThread);
			clientThread.start();
		}
		for (int i = 0; i < params.getThreadCount(); i++) {
			threads.get(i).join();
		}
		
		long totalErrors = 0;
		long totalWorkTime = 0;
		long successful = 0;
		long callsMade = 0;
		for (ClientThread t : threads){
			totalErrors = totalErrors + t.getTotalErrors();
			totalWorkTime=totalWorkTime+ t.getTotalTime();
			successful = successful+ t.getSuccessful();
			callsMade = callsMade +t.getCallNumber();
		}
		
		
		
		if (params.isCompensator() == true)
			System.out.println("COMPENSATION BASED");
		System.out.println("Time taken: "+ totalWorkTime);
		System.out.println("Calls Made: " + callsMade);
		System.out.println("Failures:   " + totalErrors);
		System.out.println("Successful: "+ successful);
		System.out.println("***************************");
	}

	@After
	public void accountCheck() throws Exception {

		int i = 0;
		int dbType = params.getDbType();

		if (dbType == DBTypes.FOUNDATIONDB
				|| dbType == DBTypes.FOUNDATIONDB_NO_RETRY) {
			i = InitializeAndCheckFDB.checkBalance();
		} else {
			i = InitializeAndCheckMongo.checkMongo();
		}

		System.out.println("Variance= " + i);
		System.out.println("***************************");
	}

	private static class ClientThread extends Thread {

		private WorkerParameters params;
		private List<String> contendedKeys;
		private long startTime = System.currentTimeMillis();
		private long runTime;
		
		private long totalErrors = 0;
		private long totalTime = 0;
		private long successful = 0;
		private long callsMade = 0;
		
		ClientThread(List<String> contendedKeys, WorkerParameters params, long runTime){
			this.contendedKeys = contendedKeys;
			this.params = params;
			this.runTime = runTime;
		}
		
		@Override
		public void run() {
			
			//Set up the relevant DBMaching to store connection and do work.	
			if (params.isCompensator()){
				
				
				//Parameter setting
				RunnerService runnerService = createWebServiceClient();
							
				//runnerService.setCollections();

				runnerService.setContendedRecords(this.contendedKeys);
				
				runnerService.setChances(params.getChanceOfRead(),
											params.getChanceOfInsert(), params.getChanceOfUpdate(),
											params.getChanceOfBalanceTransfer(),
											params.getChanceOfLogRead(), params.getChanceOfLogInsert());
				runnerService.setParams(params.getMaxTransactionSize(),
											params.getMinTransactionSize(), params.COMPENSATE_PROB,
											params.getBatchSize(), params.getMillisBetweenActions(), params.getLogReadLimit(),params.getContendedRecords());

				
				//Run the service
				runnerService.run(runTime);
				
				//Collect some results
				totalTime = runnerService.getTotalRunTime();
				callsMade = runnerService.getNumberOfCalls();
	
			        
				
			
			
			}else{
			
				DBWorker worker = new DBWorker(contendedKeys,params);
					
				Measurement m = worker.doWork(runTime);
				
				totalErrors= totalErrors +m.getErrorCount();
				totalTime = totalTime + m.getTimeTaken();
				successful =  successful + m.getSuccessful();
				callsMade = callsMade + m.getCallNumber();
			}
		}
		
		public long getTotalErrors(){
			return totalErrors;
		}

		public long getTotalTime(){
			return totalTime;
		}
		
		public long getCallNumber(){
			return callsMade;
		}
		
		public long getSuccessful(){
			return successful;
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
	    
			
		
	}
	

}


