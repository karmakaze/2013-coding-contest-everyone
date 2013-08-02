package ca.kijiji.contest;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

public class StreetMap extends TreeMap<String, Integer> {
    private static final long serialVersionUID = 6477421724158280188L;

   @Override
    public Integer get(Object key) {
	// Shortcut method: it's faster to retrieve directly into the underlying map, it does 
       // not involve use of the comparator but only of the hashcode
	return unsortedstreets.get((String) key).getProfit();
    }

    private static class StreetComparator implements Comparator<String>, Serializable {
	// Need to keep a reference to the streets here: if the StreetComparator
	// class is static, it can't access
	// the enclosing StreetMap.streets, if it's not static it can't be passed to the
	// super(comparator) constructor

	private static final long serialVersionUID = 5032739187996611946L;
	private Map<String, Street> unsortedstreets;

	@Override
	public int compare(String o1, String o2) {
	    Street s1 = unsortedstreets.get(o1);
	    if (s1==null) {
		return -1;
	    }
	    Street s2 = unsortedstreets.get(o2);
	    if (s2 == null) {
		return 1;
	    }
	    int p1 = s1.getProfit();
	    int p2 = s2.getProfit();
	    // Sort in descending order
	    return p2 - p1;
	}
    };

    private final Map<String, Street> unsortedstreets;

    /**
     * NOTE: this implementation is not robust w.r.t. to changes in the collection that is
     * passed in the constructor. If the data is changed after building the map, 
     * changes will cause unpredictable behavior of the StreetMap.
     * 
     * @param streets
     */
    public StreetMap(Map<String, Street> streets) {

	super(new StreetComparator());
	StreetComparator sc = (StreetComparator) comparator();
	this.unsortedstreets = streets;
	sc.unsortedstreets = this.unsortedstreets;
	for (Street s : streets.values()) {
	    addStreet(s);
	}
    }

    private void addStreet(Street s) {
	super.put(s.getName(), s.getProfit());
    }

    @Override
    public Integer remove(Object arg0) {
	throw new IllegalArgumentException("StreetMap is not mutable");
    }

    @Override
    public void clear() {
	throw new IllegalArgumentException("StreetMap is not mutable");
    }

    @Override
    public java.util.Map.Entry<String, Integer> pollFirstEntry() {
	throw new IllegalArgumentException("StreetMap is not mutable");
    }

    @Override
    public java.util.Map.Entry<String, Integer> pollLastEntry() {
	throw new IllegalArgumentException("StreetMap is not mutable");
    }

    @Override
    public Integer put(String key, Integer value) {
	throw new IllegalArgumentException("StreetMap is not mutable");
    }

    @Override
    public void putAll(Map<? extends String, ? extends Integer> map) {
	throw new IllegalArgumentException("StreetMap is not mutable");
    }
}
