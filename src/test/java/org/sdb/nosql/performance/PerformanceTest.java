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
import java.net.SocketTimeoutException;
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
import org.sdb.nosql.db.performance.Measurement;
import org.sdb.nosql.db.worker.DBTypes;
import org.sdb.nosql.db.worker.DBWorker;
import org.sdb.nosql.db.worker.WorkerParameters;

import com.foundationdb.Database;

@RunWith(Arquillian.class)
public class PerformanceTest {
		
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	long runTime = 600000;
	private int threadCount = 150;
	private int batchSize = 5;  //Now running based upon time, batch size is not important - so to attain the most accurate readings it is now reduced to 1
	
	private boolean isCompensator = false;
	private int dbType = DBTypes.MONGODB_COMPENSATION;
	private int contendedRecordNum = 450;	

	
	private void setTestParams() {

		
		// PAY ATTENTION HERE !
		params.setChanceOfRead(1);
		params.setChanceOfInsert(0);
		params.setChanceOfUpdate(1000);
		params.setChanceOfBalanceTransfer(0);
		params.setChanceOfLogRead(0);
		params.setChanceOfLogInsert(0);

		params.setMaxTransactionSize(3);
		params.setMinTransactionSize(3);
		params.setMillisBetweenActions(10);
	
		params.setLogReadLimit(1);
	}
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	
	
	// Test parameters
	private WorkerParameters params = 
			new WorkerParameters(dbType, // DBType
								isCompensator, // Compensatory?
								threadCount, // Thread Count
								batchSize, // Batch Size
								contendedRecordNum // Contended Records
	);
	
	private List<String> contendedKeys = null;
	
	//@Deployment
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
		
		
		
		long totalReadErrors = 0;
		long totalInsertErrors = 0;
		long totalUpdateErrors = 0;
		long totalBalTranErrors = 0;
		long totalLogReadErrors = 0;
		long totalLogWriteErrors = 0;
		
		long totalReadSuccess = 0;
		long totalInsertSuccess = 0;
		long totalUpdateSuccess = 0;
		long totalBalTranSuccess = 0;
		long totalLogReadSuccess = 0;
		long totalLogWriteSuccess = 0;
		
		for (ClientThread t : threads){
			totalErrors = totalErrors + t.getTotalErrors();
			totalWorkTime=totalWorkTime+ t.getTotalTime();
			successful = successful+ t.getSuccessful();
			callsMade = callsMade +t.getCallNumber();
			
			
			totalReadErrors = totalReadErrors + t.getTotalReadErrors();
			totalInsertErrors = totalInsertErrors + t.getTotalInsertErrors();
			totalUpdateErrors = totalUpdateErrors + t.getTotalUpdateErrors();
			totalBalTranErrors = totalBalTranErrors + t.getTotalBalTranErrors();
			totalLogReadErrors = totalLogReadErrors + t.getTotalLogReadErrors();
			totalLogWriteErrors = totalLogWriteErrors + t.getTotalLogWriteErrors();
			
			totalReadSuccess = totalReadSuccess + t.getTotalReadSuccess();
			totalInsertSuccess = totalInsertSuccess + t.getTotalInsertSuccess();
			totalUpdateSuccess = totalUpdateSuccess + t.getTotalUpdateSuccess();
			totalBalTranSuccess = totalBalTranSuccess + t.getTotalBalTranSuccess();
			totalLogReadSuccess = totalLogReadSuccess + t.getTotalLogReadSuccess();
			totalLogWriteSuccess = totalLogWriteSuccess + t.getTotalLogWriteSuccess();	
			
		}
		
		
		System.out.println("OVERALL OUTCOME: ");
		if (params.isCompensator() == true)
			System.out.println("COMPENSATION BASED");
		System.out.println("Time taken:                "+ totalWorkTime);
		System.out.println("Calls Made:                " + callsMade);
		System.out.println("Failures:                  " + totalErrors);
		System.out.println("Successful:                "+ successful);
		System.out.println("***************************");
		System.out.println("SCECIFIC OUTCOMES: ");
		
		if (totalReadErrors > 0 || totalReadSuccess > 0){
			System.out.println("Read Success:              " + totalReadSuccess);
			System.out.println("Read Failuers:             " + totalReadErrors);
		}
		
		if (totalInsertErrors > 0 || totalInsertSuccess > 0){
			System.out.println("Insert Success:            " + totalInsertSuccess);
			System.out.println("Insert Failuers:           " + totalInsertErrors);
		}
		
		if (totalUpdateErrors > 0 || totalUpdateSuccess > 0){
			System.out.println("Update Success:            " + totalUpdateSuccess);
			System.out.println("Update Failuers:           " + totalUpdateErrors);
		}
		
		if (totalBalTranErrors > 0 || totalBalTranSuccess > 0){
			System.out.println("Balance Transfer Success:  " + totalBalTranSuccess);
			System.out.println("Balance Transfer Failures: " + totalBalTranErrors);
		}
		
		if (totalLogReadErrors > 0 || totalLogReadSuccess > 0){
			System.out.println("Log Read Success:          " + totalLogReadSuccess);
			System.out.println("Log Read Failuers:         " + totalLogReadErrors);
		}
		
		if (totalLogWriteErrors > 0 || totalLogWriteSuccess > 0){
			System.out.println("Log Write Success:         " + totalLogWriteSuccess);
			System.out.println("Log Write Failuers:        " + totalLogWriteErrors);
		}
	}

	@After
	public void accountCheck() throws Exception {

		
		if (params.getChanceOfBalanceTransfer()>0){
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
	}

	private static class ClientThread extends Thread {

		private WorkerParameters params;
		private List<String> contendedKeys;
		private long runTime;
		
		private long totalErrors = 0;
		private long totalTime = 0;
		private long successful = 0;
		private long callsMade = 0;

		public long getTotalReadErrors() {
			return totalReadErrors;
		}

		public long getTotalInsertErrors() {
			return totalInsertErrors;
		}

		public long getTotalUpdateErrors() {
			return totalUpdateErrors;
		}

		public long getTotalBalTranErrors() {
			return totalBalTranErrors;
		}

		public long getTotalLogReadErrors() {
			return totalLogReadErrors;
		}

		public long getTotalLogWriteErrors() {
			return totalLogWriteErrors;
		}

		public long getTotalReadSuccess() {
			return totalReadSuccess;
		}

		public long getTotalInsertSuccess() {
			return totalInsertSuccess;
		}

		public long getTotalBalTranSuccess() {
			return totalBalTranSuccess;
		}

		public long getTotalUpdateSuccess() {
			return totalUpdateSuccess;
		}

		public long getTotalLogReadSuccess() {
			return totalLogReadSuccess;
		}

		public long getTotalLogWriteSuccess() {
			return totalLogWriteSuccess;
		}

		private long totalReadErrors;
		private long totalInsertErrors;
		private long totalUpdateErrors;
		private long totalBalTranErrors;
		private long totalLogReadErrors;
		private long totalLogWriteErrors;
		private long totalReadSuccess;
		private long totalInsertSuccess;
		private long totalBalTranSuccess;
		private long totalUpdateSuccess;
		private long totalLogReadSuccess;
		private long totalLogWriteSuccess;
		
		ClientThread(List<String> contendedKeys, WorkerParameters params, long runTime){
			this.contendedKeys = contendedKeys;
			this.params = params;
			this.runTime = runTime;
		}
		
		@Override
		public void run() {
			
			DBWorker worker= null;
			
			RunnerService runnerService =null;
			
			
			//Set up the DB worker
			if (params.isCompensator()){
				
				//The db worker is setup on the receiver in these next calls. 
				runnerService = createWebServiceClient();
							
				runnerService.setContendedRecords(this.contendedKeys);
				
				runnerService.setChances(params.getChanceOfRead(),
											params.getChanceOfInsert(), params.getChanceOfUpdate(),
											params.getChanceOfBalanceTransfer(),
											params.getChanceOfLogRead(), params.getChanceOfLogInsert());
				runnerService.setParams(params.getMaxTransactionSize(),
											params.getMinTransactionSize(), params.COMPENSATE_PROB,
											params.getBatchSize(), params.getMillisBetweenActions(), params.getLogReadLimit(),params.getContendedRecords());

			}else{
				 //Don't need to hook into Wildfly, so we can just setup a worker here. 
				 worker = new DBWorker(contendedKeys,params);
			}
			
			
			//Run the workload. This is where the real test happens.
			long startTime = System.currentTimeMillis();
			while (System.currentTimeMillis() < startTime+ runTime){
				if (params.isCompensator()){
					
					//Run the service
					runnerService.run();
					
					//Collect some results
					totalTime = totalTime+ runnerService.getTotalRunTime();
					callsMade = callsMade+ runnerService.getNumberOfCalls();
					successful = successful + runnerService.getTotalSuccess();
					totalErrors = totalErrors + runnerService.getTotalFail();
				
				}else{
									
					Measurement m = worker.doWork();
					
					totalErrors= totalErrors + m.getErrorCount();
					totalTime = totalTime + m.getTimeTaken();
					successful =  successful + m.getSuccessful();
					callsMade = callsMade + m.getCallNumber();
					
					totalReadErrors = totalReadErrors + m.getFailedReads();
					totalInsertErrors = totalInsertErrors + m.getFailedInserts();
					totalUpdateErrors = totalUpdateErrors + m.getFailedUpdates();
					totalBalTranErrors = totalBalTranErrors + m.getFailedBalTrans();
					totalLogReadErrors = totalLogReadErrors + m.getFailedLogReads();
					totalLogWriteErrors = totalLogWriteErrors + m.getFailedLogWrites();
					
					totalReadSuccess = totalReadSuccess + m.getSuccessfulReads();
					totalInsertSuccess = totalInsertSuccess + m.getSuccessfulInserts();
					totalUpdateSuccess = totalUpdateSuccess + m.getSuccessfulUpdates();
					totalBalTranSuccess = totalBalTranSuccess + m.getSuccessfulBalTrans();
					totalLogReadSuccess = totalLogReadSuccess + m.getSuccessfulLogReads();
					totalLogWriteSuccess = totalLogWriteSuccess + m.getSuccessfulLogWrites();
					
					
					
				}
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


