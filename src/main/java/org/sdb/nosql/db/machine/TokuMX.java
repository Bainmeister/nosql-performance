package org.sdb.nosql.db.machine;

import org.sdb.nosql.db.connection.MongoConnection;

public class TokuMX extends Mongo {

	//NOTE: TokuMX calls aren't any different from standard MongoDB calls,
	// so this is basically here for readability (and future changes). 
	public TokuMX(MongoConnection connection) {
		super(connection);
	}
	
}
