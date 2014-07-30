package org.sdb.nosql.db.machine;

import org.sdb.nosql.db.connection.MongoConnection;
import com.mongodb.BasicDBObject;

public class TokuMXTransactionalMVCC extends TokuMXTransactional{

	public TokuMXTransactionalMVCC(MongoConnection connection) {
		super(connection);
	}
	
	//Lets explicitly state we want an MVCC transaction
	BasicDBObject beginTransaction(){
		
		//Create beginTransaction object
		BasicDBObject beginTransaction = new BasicDBObject();
		beginTransaction.append("beginTransaction", 1);
		beginTransaction.append("isolation", "MVCC");
		
		return beginTransaction;
	}

}
