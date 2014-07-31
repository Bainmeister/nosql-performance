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

import java.io.File;
import java.security.KeyStore;
import java.util.List;

import io.narayana.perf.Result;
import io.narayana.perf.WorkerWorkload;

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
import org.sdb.nosql.db.connection.DBConnection;
import org.sdb.nosql.db.connection.MongoConnection;
import org.sdb.nosql.db.keys.generation.KeyGen;
import org.sdb.nosql.db.machine.DBMachine;
import org.sdb.nosql.db.performance.ActionRecord;
import org.sdb.nosql.db.worker.DBTypes;
import org.sdb.nosql.db.worker.DBWorker;
import org.sdb.nosql.db.worker.WorkerParameters;

import com.foundationdb.Database;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;

@RunWith(Arquillian.class)
public class PerformanceTest1 {	
	
	//Test parameters
	private WorkerParameters params = new WorkerParameters(		DBTypes.FOUNDATIONDB_NO_RETRY,  	//DB Type
																true, 				//Compensatory?
																10, 				//Thread Count
																500, 				//Number of Calls
																10, 				//Batch Size
																2					//Contended Records
															);
	private void setTestParams(){
		
		params.setChanceOfRead(0);
		params.setChanceOfInsert(1000);
		params.setChanceOfUpdate(0);
		params.setChanceOfBalanceTransfer(0);
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
		
		//archive.addClass(DBWorker.class);
		
		//archive.addPackages(true, "org.jboss.sdb.nosqltest.perf");
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
		InitializeAndCheckMongo.setupMongo(params.getContendedRecords());
		
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
		
		//1) Connect to the DB and grab some keys
		List<String> contendedKeys = new KeyGen(new MongoConnection()).getKeys(params.getContendedRecords());
		
		//2) Setup the template worker using the contended keys + other parameters
		DBWorker<Void> workerTemplate = new DBWorker<Void>(contendedKeys,params);

		//3) Run the test with a warm up cycle of 100
		Result<Void> measurement = new Result<Void>(params.getThreadCount(), 
													params.getNumberOfCalls(), 
													params.getBatchSize())
													.measure(workerTemplate, workerTemplate, 100);
		
		
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

		int i = InitializeAndCheckMongo.checkMongo();
	
		
		System.out.println("variance= "+ i);
		System.out.println("***************************");
	}
}
