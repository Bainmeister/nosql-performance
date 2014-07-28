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
package org.sdb.nosql.db.machine;

import java.util.HashMap;
import java.util.List;

import org.sdb.nosql.db.performance.ActionRecord;


/**
 * @author <a href="mailto:s.bain@newcastle.ac.uk">S Bain</a>
 *
 * Interface for DB interaction tests.
 */
public interface DBMachine {
	
	/**
	 * Get numberOfKeys from the database
	 * @param numberOfkeys
	 * @return
	 */
	HashMap<String, String> getKeysFromDB(int numberOfkeys);
	
	/**
	 * Performs (transactionSize) reads against the database that is currently connected.
	 *  
	 * @param  keyLength The length of a key - "00" = 2, "000" = 3, "0000" = 4
	 * @param  transActionSize The number of actions within the transaction
	 * @return the attempts taken to complete the transaction
	 */
	ActionRecord read(List<String> keys, int waitMillis);
		
	/**
	 * Performs (transactionSize) updates to the database that is currently connected.
	 *  
	 * @param  keyLength The length of a key - "00" = 2, "000" = 3, "0000" = 4
	 * @param  transActionSize The number of actions within the transaction
	 * @return the attempts taken to complete the transaction
	 */
	ActionRecord update(List<String> keys, int waitMillis);
	
	/**
	 * Performs an insert to the db;
	 * @return
	 */
	ActionRecord insert(List<String> values, int waitMillis);
	
	/**
	 * Performs (transactionSize) read and write operations to the database that is 
	 * currently connected.
	 *  
	 * @param  keyLength The length of a key - "00" = 2, "000" = 3, "0000" = 4
	 * @param  transActionSize The number of actions within the transaction
	 * @return the attempts taken to complete the transaction
	 */
	ActionRecord readModifyWrite(List<String> keys, int waitMillis);
	
	ActionRecord balanceTransfer(String key1, String key2, int waitMillis);
	
	ActionRecord incrementalUpdate(List<String> keys, int waitMillis);
	
	ActionRecord writeLog(int numberToWrite, int waitMillis);
	
	ActionRecord readLog(int numberToRead, int waitMillis);
	
	void addTable(String name);
	
}
