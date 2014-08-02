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

import io.narayana.perf.WorkerWorkload;

import java.io.File;
import java.security.KeyStore;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javax.inject.Inject;

import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.narayana.compensations.api.TransactionCompensatedException;
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
import org.sdb.nosql.db.connection.DBConnection;
import org.sdb.nosql.db.connection.FoundationConnection;
import org.sdb.nosql.db.connection.MongoConnection;
import org.sdb.nosql.db.keys.generation.KeyGen;
import org.sdb.nosql.db.machine.DBMachine;
import org.sdb.nosql.db.machine.Mongo;
import org.sdb.nosql.db.performance.ActionRecord;
import org.sdb.nosql.db.worker.DBTypes;
import org.sdb.nosql.db.worker.DBWorker;
import org.sdb.nosql.db.worker.Measusement;
import org.sdb.nosql.db.worker.WorkerParameters;

import com.foundationdb.Database;
import com.mongodb.DBCollection;

@RunWith(Arquillian.class)
public class PerformanceTest {

	// Test parameters
	private WorkerParameters params = 
			new WorkerParameters(DBTypes.TOKUMX, // DBType
								true, // Compensatory?
								10, // Thread Count
								10, // Number of Calls
								10, // Batch Size
								2 // Contended Records
	);

	long runTime =1000;
	
	private void setTestParams() {

		params.setChanceOfRead(0);
		params.setChanceOfInsert(0);
		params.setChanceOfUpdate(0);
		params.setChanceOfBalanceTransfer(10000);
		params.setChanceOfLogRead(0);
		params.setChanceOfLogInsert(0);

		params.setMaxTransactionSize(2);
		params.setMinTransactionSize(2);
		params.setMillisBetweenActions(0);
	}

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
								WorkerWorkload.class.getPackage().getName(),
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
		System.out.println("Calls:      " + params.getNumberOfCalls());
		System.out.println("***************************");

		// Pre-test
		setTestParams();
		List<String> contendedKeys = null;

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
			DBWorker worker = new DBWorker(contendedKeys,params);
			
			while (System.currentTimeMillis() < startTime+ runTime){
				Measusement m = worker.doWork(params.getBatchSize());
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
		
	}

}


