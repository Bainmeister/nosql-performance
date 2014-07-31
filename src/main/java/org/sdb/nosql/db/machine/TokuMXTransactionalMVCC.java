package org.sdb.nosql.db.machine;

import org.sdb.nosql.db.connection.MongoConnection;
import com.mongodb.BasicDBObject;

public class TokuMXTransactionalMVCC extends TokuMXTransactional {

	public TokuMXTransactionalMVCC(MongoConnection connection) {
		super(connection);
	}

	// Lets explicitly state we want an MVCC transaction
	@Override
	BasicDBObject beginTransaction() {
		return beginTransaction("MVCC");
	}

}
