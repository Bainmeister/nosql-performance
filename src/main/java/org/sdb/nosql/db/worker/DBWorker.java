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
import java.util.List;

import javax.xml.namespace.QName;
import javax.xml.ws.Service;

import io.narayana.perf.Result;
import io.narayana.perf.Worker;

import org.sdb.nosql.db.compensation.javax.RunnerService;
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
	
	//Construct
	public DBWorker(List<String> contendedRecords, WorkerParameters params){
		this.contendedRecords = contendedRecords;
		this.params = params;
		
		//now connect to the required db and setup relevant database machine.
		//Call the doWork 
		if (!params.isCompensator){
			
		}
	}
			
	
	@Override
	public T doWork(T context, int batchSize, Result<T> measurement) {
		
		long timetaken = 0;
		
		//Call the doWork 
		if (params.isCompensator){
			
			//TODO add and actionRecord to pass back rather than workTimeMillis
	        RunnerService runnerService = createWebServiceClient();
	        workTimeMillis = runnerService.doWork(params);
	        workTimeMillis  = timetaken;
	        return null;
		}
		
		
		

		
		System.out.println("time taken: " + timetaken);
		
		return null;
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