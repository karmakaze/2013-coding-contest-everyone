package ca.kijiji.contest;

import java.util.TreeMap;


public class SortedCountMap<K, V> extends TreeMap<K, Integer> {

	private static final long serialVersionUID = -6233514320502923598L;

	@Override
    public Integer put(K key, Integer incrementValue) {
        Integer insertValue = super.get(key);
        if (insertValue == null) {
        	insertValue = 0;            
        }
        insertValue += incrementValue;
        super.put(key, insertValue);
        return insertValue;
    }    
}	
