package org.sdb.nosql.db.compensation;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

import org.jboss.narayana.compensations.api.ConfirmationHandler;

import javax.inject.Inject;

/**
 * @author paul.robinson@redhat.com 28/06/2014
 */
public class ConfirmIncrement implements ConfirmationHandler {

    @Inject
    IncrementCounterData incrementCounterData;

    @Override
    public void confirm() {

        MongoClient mongoClient = CounterManager.getMongoClient();
        DB database = mongoClient.getDB("test");
        DBCollection accounts = database.getCollection("counters");

        accounts.update(new BasicDBObject("name", String.valueOf(incrementCounterData.getCounter())), new BasicDBObject("$inc", new BasicDBObject("tx", 1)));
    }
}