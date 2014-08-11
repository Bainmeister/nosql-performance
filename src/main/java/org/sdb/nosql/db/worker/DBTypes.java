package org.sdb.nosql.db.worker;

public class DBTypes {
	
	final public static int NOT_SET= 0;
	
	final public static int FOUNDATIONDB = 10;
	final public static int FOUNDATIONDB_NO_RETRY =12;
	
	final public static int MONGODB = 20;
	final public static int MONGODB_COMPENSATION = 25;
	
	final public static int TOKUMX = 30;
	final public static int TOKUMX_TRANS = 31;
	final public static int TOKUMX_TRANS_MVCC = 32;
	final public static int TOKUMX_TRANS_SERIALIABLE = 33;
	final public static int TOKUMX_TRANS_BoB = 34;
	final public static int TOKUMX_TRANS_BOB = 34;
}
