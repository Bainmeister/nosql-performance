package org.sdb.nosql.db.worker;

public class DBTypes {
	
	final public static int NOT_SET= 0;
	
	final public static int FOUNDATIONDB = 10;
	final public static int FOUNDATIONDB_BLOCK_NO_RETRY=11;
	final public static int FOUNDATIONDB_NO_RETRY=12;
	//final public static int FDB_COMPENSATION =13;
	
	final public static int MONGODB = 20;
	final public static int MONGODB_COMPENSATION = 20;
	final public static int TOKUMX = 30;
	final public static int TOKUMX_ACID_OC = 31;
	final public static int TOKUMX_ACID_PC = 32;
	final public static int TOKUMX_ACID_MIX = 33;
	final public static int TOKUMX_COMPENSATION = 34;
	
}
