package org.sdb.nosql.performance;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

import com.foundationdb.*;
import com.foundationdb.async.Function;
import com.foundationdb.tuple.Tuple;

public class InitializeAndCheckFDB {

  private static final FDB fdb;
  private static final Database db;

  static {
    fdb = FDB.selectAPIVersion(200);
    db = fdb.open();
  }
  
  // Generate Predictable Keys
  private static List<String> units = Arrays.asList("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");

  
  private static List<String> indexes = initKeys(3);
  
  /**
   * @return list of keys
   */
  private static List<String> initKeys(int figures) {
	  	    
	  List<String> keyList = new ArrayList<String>();
	  
	  //For now, just return 0000 - 9999
	  for (String level4: units)
		  for (String level3: units)
			  for (String level2: units)
				  for (String level1: units)
					  keyList.add(level4 + level3 + level2 + level1);
	        				
    
    return keyList;
  }
 
  private static void addBalanceKey( Transaction tr, final String c) {	  
	  
	  try{
		  tr.set(Tuple.from("balance", c).pack(), encodeInt(1000));
	  }catch (FDBException e){
		  e.printStackTrace();
	  }catch (IllegalArgumentException ie){
		  ie.printStackTrace();
	  }
	  
	  /*db.run(new Function<Transaction, Void>() {
	      public Void apply(Transaction tr) {
	    	  
	    	  try{
	    		  //dry an account to the value of 1000
	    		  tr.set(Tuple.from("balance", c).pack(), encodeInt(1000));
	    	  }catch (FDBException e){
	    		  e.printStackTrace();
	    	  }
	    	  
	    	  
	    	  return null;
	      }
	    });	*/  
  }
  public static int checkBalance() {
	return db.run(new Function<Transaction, Integer>() {
	      public Integer apply(Transaction tr) {
	        
	    	  int i = 0;
	    	  try{
	    		//dry an account to the value of 1000
	    		//tr.set(Tuple.from("balance", c).pack(), encodeInt(1000));
		    	for(KeyValue kv: tr.getRange(Tuple.from("balance").range())){
		    		
		    		int temp = decodeInt(kv.getValue());	
		    		i = i +temp;
		    		
		    	//	if (temp!= 1000)
		    			//System.out.println(temp);
		    	}
	    		  
	    	  }catch (FDBException e){
	    		  e.printStackTrace();
	    	  }
	    	  
	    	  return i;
	      }
	    });
  }
  /**
   * encode and int ready for FDB storage
   * @param value
   * @return
   */
	private static byte[] encodeInt(int value) {
		byte[] output = new byte[4];
		ByteBuffer.wrap(output).putInt(value);	
		return output;
	}
	
	/**
	 * Decode a byte into an int from FDB storage.
	 * @param value
	 * @return
	 */
	private static int decodeInt(byte[] value) {
		if (value.length != 4)
			throw new IllegalArgumentException("Array must be of size 4");
		return ByteBuffer.wrap(value).getInt();
	}
  
  
  public static void initFDB() {
	  
	  
	  
	  db.run(new Function<Transaction, Void>() {
	    
	    public Void apply(Transaction tr) {
	     
	     tr.clear(Tuple.from("balance").range());    
	     tr.clear(Tuple.from("log1").range());   		
	     tr.clear(Tuple.from("log2").range());
	     tr.clear(Tuple.from("log3").range()); 
	     
		 for (String key: indexes)
			 addBalanceKey(tr, key);    
	      return null;
	    }
	  });
	}
  
}