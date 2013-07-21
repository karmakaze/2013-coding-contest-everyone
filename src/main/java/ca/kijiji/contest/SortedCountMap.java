package ca.kijiji.contest;

import java.util.TreeMap;


public class SortedCountMap<K, V> extends TreeMap<String, Integer> {

	private static final long serialVersionUID = -6233514320502923598L;
	String largestFineSumStreet = null;
	int largestFineSum = 0;
	
	@Override
    public Integer put(String key, Integer incrementValue) {
        Integer insertValue = super.get(key);
        if (insertValue == null) {
        	insertValue = 0;            
        }
        insertValue += incrementValue;
        
        if (insertValue > largestFineSum) {
        	largestFineSum = insertValue;
        	largestFineSumStreet = key;
        }
        
        super.put(key, insertValue);
        return insertValue;
    }   
	
	// TODO: Is this really the best way? Ugh.
	public String firstKey() {
		return largestFineSumStreet;
	}
}	
