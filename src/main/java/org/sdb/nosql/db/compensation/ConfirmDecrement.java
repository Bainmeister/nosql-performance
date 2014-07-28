package org.sdb.nosql.db.compensation;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

import org.jboss.narayana.compensations.api.ConfirmationHandler;

import javax.inject.Inject;

/**
 * This compensation handler is used to undo a credit operation.
 *
 * @author paul.robinson@redhat.com 09/01/2014
 */
public class ConfirmDecrement implements ConfirmationHandler {

    @Inject
    DecrementCounterData decrementCounterData;

    @Override
    public void confirm() {

        MongoClient mongoClient = CounterManager.getMongoClient();
        DB database = mongoClient.getDB("test");
        DBCollection accounts = database.getCollection("counters");

        accounts.update(new BasicDBObject("name", String.valueOf(decrementCounterData.getCounter())), new BasicDBObject("$inc", new BasicDBObject("tx", 1)));
    }
}
