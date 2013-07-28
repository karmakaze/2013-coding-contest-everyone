/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ca.kijiji.contest;

import java.util.Comparator;
import java.util.Map;

/**
 *
 * @author eamonn
 */
public class ValueComparator implements Comparator {
    
    Map map;
    
    public ValueComparator(Map map){
        this.map = map;
    }
    
    public int compare(Object keyX, Object keyY){
 
        Comparable x = (Comparable) map.get(keyX);
        Comparable y = (Comparable) map.get(keyY);
  
        int res = y.compareTo(x);
        
        return res;
    }
 
}
