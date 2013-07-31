package ca.kijiji.contest;

import java.util.Comparator;
import java.util.Map;

/**
 * Custom comparator to sort a Map to values in descending order.
 * 
 * @author Eamonn Watson
 */
public class ValueComparator implements Comparator {
    
    Map map;
    
    /**
     * creates the local map
     * @param map
     */    
    public ValueComparator(Map map){
        this.map = map;
    }
    
        /**
     * Takes the location input from the file, parses it to remove just the street name.
     * 
     * @param keyX - The 1st object to compare
     * @param keyY - the 2nd object to compare
     * @return comparison between the 2 objects
     */    
    public int compare(Object keyX, Object keyY){
 
        Comparable x = (Comparable) map.get(keyX);
        Comparable y = (Comparable) map.get(keyY);
  
        int res = y.compareTo(x);
        
        return res;
    }
 
}
