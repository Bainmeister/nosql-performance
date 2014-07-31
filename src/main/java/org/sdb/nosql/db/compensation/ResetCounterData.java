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
public class ResetCounterData implements Serializable {

    private int counter;
    private int originalAmount;
    private String oID;
    
    public ResetCounterData() {}

    public void setCounterAndOriginalAmount(String counter, int originalAmount){
    	this.oID = counter;
    	this.originalAmount = originalAmount;
    }
    
    public void setCounter(int counter) {

        this.counter = counter;
    }

    public void setOriginalAmount(int amount) {

        this.originalAmount = amount;
    }

    public int getCounter() {

        return counter;
    }

    public int getOrinalAmount() {

        return originalAmount;
    }

	public String getoID() {
		return oID;
	}

	public void setoID(String oID) {
		this.oID = oID;
	}
}