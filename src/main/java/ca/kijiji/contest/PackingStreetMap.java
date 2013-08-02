package ca.kijiji.contest;

import java.util.Collection;
import java.util.Hashtable;

/**
 * This kind of map is used for holding Street indexed by street names. 
 * 
 * A simple String/Street map with convenience methods for adding streets summing the profit.
 * It extends Hashtable and not HashMap because Hashtable is synchronized. 
 * It's not a real issue in the test case because the threads that modify the map
 * use only the synchronized method defined below (addAll), while the only thread that retrieves
 * data is the main thread that gets data only after the map has been filled by all runner threads.
 * However, Hashtable does not decrease performance in this context, so it just seems more logical to extend it
 * on a shared object.
 * 
 *   NOTE: it could as well be a collection and not a dictionary, but returning hashcode on Street()
 *   on the basis of street name only, would break the contract with equals, unless two streets are considered
 *   to be equal even when the profits differ. And this doesn't look very good to me :-)
 * @author Chiara
 *
 */
public final class PackingStreetMap extends Hashtable<String, Street> {
    private static final long serialVersionUID = -860217198117798725L;

    /**
     * It's not strictly necessary for this method to be synchronized, however 
     * it is called only once per thread at the end of processing, therefore there's
     * probably no reason to release the lock until the add has been completed 
     * @param streets
     */
    public synchronized void addAll(Collection<Street> streets) {
	for (Street s:streets) {
	    add(s);
	}
    }
    
    public synchronized void add(Street s) {
	Street my = get(s.getName());
	if (my == null) {
	    // Create a new Street, do not put the original one, or subsequent 
	    // updates will change its original profit.
	    put(s.getName(), new Street(s));
	} else {
	    my.addProfit(s);
	}
    }
}