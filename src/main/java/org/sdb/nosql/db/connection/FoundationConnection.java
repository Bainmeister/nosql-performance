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
package org.sdb.nosql.db.connection;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.sdb.nosql.db.performance.ActionRecord;

import com.foundationdb.Database;
import com.foundationdb.FDB;
import com.foundationdb.KeyValue;
import com.foundationdb.Transaction;
import com.foundationdb.async.Function;
import com.foundationdb.tuple.Tuple;

/**
 * @author <a href="mailto:s.bain@newcastle.ac.uk">Simon Bain</a>
 *
 * A FoundationDB specific implementation of DBMachine. Runs the API as standard.
 */
public class FoundationConnection implements DBConnection{

    //private Database db;
    private FDB fdb;
    private Database db;

    /**
	 * @return the fdb
	 */
	public FDB getFdb() {
		return fdb;
	}

	/**
	 * @return the db
	 */
	public Database getDb() {
		return db;
	}

	public FoundationConnection(){
    	connectDB();
    }
    
	public void connectDB() {
      fdb = FDB.selectAPIVersion(200);
      db = fdb.open(); 
	}

	public void disconnectDB() {
		// Don't need to worry about this in FDB - just set to null. 
		db =null;
		fdb = null;
	}

	@Override
	public boolean isConnected() {
		return (db == null || fdb == null) == true ? false : true ;
	}
	
	
}
