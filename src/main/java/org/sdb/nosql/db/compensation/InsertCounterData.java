package org.sdb.nosql.db.compensation;

import org.jboss.narayana.compensations.api.CompensationScoped;

import java.io.Serializable;

/**
 * This is a CompensationScoped POJO that is used to store data against the current running compensating transaction.
 *
 * This scope is also available to the compensation handlers.
 *
 * @author paul.robinson@redhat.com 09/01/2014
 */
@CompensationScoped
public class InsertCounterData implements Serializable {

    private int counter;
    private int amount;
    private String oID;
    
    public InsertCounterData() {}

    public void setCounterAndAmount(String counter, int amount){
    	this.oID = counter;
    	this.amount = amount;
    }
    
    public void setCounter(int counter) {

        this.counter = counter;
    }

    public void setAmount(int amount) {

        this.amount = amount;
    }

    public int getCounter() {

        return counter;
    }

    public int getAmount() {

        return amount;
    }

	public String getoID() {
		return oID;
	}

	public void setoID(String oID) {
		this.oID = oID;
	}
}