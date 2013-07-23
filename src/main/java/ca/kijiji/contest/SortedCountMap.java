package ca.kijiji.contest;

import java.util.TreeMap;


/**
 * A map that keeps tally of the sum of the integer values inserted into each key.
 * It handles if the key doesn't exist. 
 */
public class SortedCountMap<K, V> extends TreeMap<String, Integer> {

	private static final long serialVersionUID = -6233514320502923598L;
	
	@Override
    public Integer put(String key, Integer incrementValue) {
        Integer insertValue = super.get(key);
        if (insertValue == null) {
        	insertValue = 0;            
        }
        insertValue += incrementValue;
                
        super.put(key, insertValue);
        return insertValue;
    }   
}	
