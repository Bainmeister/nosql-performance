package org.sdb.nosql.db.compensation;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;

import org.jboss.narayana.compensations.api.CompensationHandler;

import javax.inject.Inject;

/**
 * This compensation handler is used to undo a credit operation.
 *
 * @author paul.robinson@redhat.com 09/01/2014
 */
public class UndoInsert implements CompensationHandler {

    @Inject
    InsertCounterData insertCounterData;

    @Override
    public void compensate() {

        MongoClient mongoClient = CounterManager.getMongoClient();
        DB database = mongoClient.getDB("test");
        DBCollection accounts = database.getCollection("counters");

        try{
        	accounts.remove(new BasicDBObject("name", String.valueOf(insertCounterData.getCounter())));
		} catch (MongoException e){

		}
    }
}
