package ca.kijiji.contest;

import java.util.Comparator;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: darren
 * Date: 31/07/13
 * Time: 11:39 PM
 * To change this template use File | Settings | File Templates.
 */
//Comparator to sort by value instead of key
class StreetComparator implements Comparator<String> {
    Map<String, Integer> base;

    public StreetComparator(Map<String, Integer> base) {
        this.base = base;
    }

    public int compare(String left, String right) {
        return base.get(right) - base.get(left);
    }
}
