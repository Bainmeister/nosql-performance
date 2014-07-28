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
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sdb.nosql.db.compensation.CounterService;
import org.sdb.nosql.db.compensation.javax.RunnerService;
import org.sdb.nosql.db.connection.FoundationConnection;
import org.sdb.nosql.db.connection.MongoConnection;
import org.sdb.nosql.db.keys.generation.KeyGen;
import org.sdb.nosql.db.keys.storage.KeyStore;
import org.sdb.nosql.db.worker.DBWorker;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;

@RunWith(Arquillian.class)
public class PerformanceTest {

	//***********************************************************
	int threadCount = 10;
	int numberOfCalls = 1000;
	int batchSize = 100;

	//int keyLength = 2;
	//int dbType = DBWorker.TOKUMX;

	int chanceOfRead = 1;
	int chanceOfWrite =0;
	int chanceOfBalanceTransfer = 999;

	int chanceOfReadModifyWrite = 0;
	int chanceOfIncrementalUpdate =0; 

	int minTransactionSize = 2;
	int maxTransactionSize = 2; 

	int millisBetweenActions = 0;	
	int contendedRecords =3; 	//the more records, the lower the contention

	int writeToLogs = 0;
	//***********************************************************

	private DBCollection counters;

	private static final int COUNTERS = 5;
	private static final int ITERATIONS = 100;
	private static final int COMPENSATE_PROB = 0;


	@Deployment
	public static WebArchive createTestArchive() {

		//Use 'Shrinkwrap Resolver' to include the mongodb java driver in the deployment
		File lib = Maven.resolver().loadPomFromFile("pom.xml").resolve("org.mongodb:mongo-java-driver:2.10.1").withoutTransitivity().asSingleFile();

		WebArchive archive = ShrinkWrap.create(WebArchive.class, "test.war")
				.addPackages(true, CounterService.class.getPackage().getName())
				.addAsManifestResource("services/javax.enterprise.inject.spi.Extension")
				.addAsWebInfResource(EmptyAsset.INSTANCE, "beans.xml")
				.addAsLibraries(lib);

		archive.delete(ArchivePaths.create("META-INF/MANIFEST.MF"));

		final String ManifestMF = "Manifest-Version: 1.0\n"
				+ "Dependencies: org.jboss.narayana.txframework\n";
		archive.setManifest(new StringAsset(ManifestMF));
		
		archive.addClass(DBWorker.class);
		
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

		MongoClient mongo = new MongoClient("localhost", 27017);
		DB database = mongo.getDB("test");

		database.getCollection("counters").drop();
		counters = database.getCollection("counters");

		for (int i=1; i < COUNTERS+1; i++) {
			counters.insert(new BasicDBObject("name", String.valueOf(i)).append("value", 0).append("tx", 0));
		}
	}

	@Test
	public void perf() throws Exception {
		
		RunnerService runnerService = createWebServiceClient();

		List<String> contendedKeys= generateContendedKeys();
		
		//Setup the template worker
		//DBWorker<Void> worker = new DBWorker<Void>(contendedKeys);

		//FoundationConnection conn = new FoundationConnection();
		MongoConnection conn = new MongoConnection();
		
		//KeyStore ks = new KeyGen(conn).getKeys(contendedRecords);
		
		//keyGen.addKeysFromDB(5,30);
		//System.out.println(ks.getKey(0));
		//System.out.println(ks.getKey(1));
		//System.out.println(ks.getKey(3));
		
		//KeyStore keyGen = new FDBKeys();   
		//KeyStore keyGen = new MongoKeys(); 
		//keyGen.addKeysFromDB(3,30);

		//keys.addAll(cursor);


		long timetaken= runnerService.balanceTransfer(ITERATIONS, contendedKeys, COMPENSATE_PROB);
		System.out.println("***************************");
		System.out.println("time taken:    "  + timetaken);
		System.out.println("***************************");
	}

	private KeyStore KeyGenerator(FoundationConnection conn) {
		// TODO Auto-generated method stub
		return null;
	}


	private List<String> generateContendedKeys() throws UnknownHostException {
		
		MongoClient mongo = new MongoClient("localhost", 27017);
		DB database = mongo.getDB("test");
		counters = database.getCollection("counters");

		List<String> keys = new ArrayList<String>();
		DBCursor cursor = counters.find().limit(5);
		
		try {
			while(cursor.hasNext()) {
				keys.add((String) cursor.next().get("name"));
			}
		} finally {
			cursor.close();
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


}
