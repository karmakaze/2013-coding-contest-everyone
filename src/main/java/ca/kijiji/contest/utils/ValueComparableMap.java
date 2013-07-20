package ca.kijiji.contest.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import com.google.common.base.Functions;
import com.google.common.collect.Ordering;

/**
 * 
 * Minor modification to the implementation mentioned  <a href=
 * "http://stackoverflow.com/questions/109383/how-to-sort-a-mapkey-value-on-the-values-in-java/3420912#3420912"
 * > Here </a>.<br>
 * This modification is required when using this map is used for live sorting.
 * 
 * @param <K>
 * @param <V>
 */
public class ValueComparableMap<K extends Comparable<K>, V> extends
		TreeMap<K, V> {
	private static final long serialVersionUID = 1L;
	// A map for lookup
	private final Map<K, V> valueMap;

	public ValueComparableMap(final Ordering<? super V> partialValueOrdering) {
		this(partialValueOrdering, new HashMap<K, V>());
	}

	private ValueComparableMap(Ordering<? super V> partialValueOrdering,
			HashMap<K, V> valueMap) {
		super(partialValueOrdering // Apply the value ordering
				.onResultOf(Functions.forMap(valueMap)) // On the result of
														// getting the value for
														// the key from the map
				.compound(Ordering.natural())); // as well as ensuring that the
												// keys don't get clobbered
		this.valueMap = valueMap;
	}
	
	public boolean containsKey(K key) {
		return valueMap.containsKey(key);
	}


	public V put(K k, V v) {
		if (valueMap.containsKey(k)) {
			// remove the key in the sorted set before adding the key again
			remove(k);
		}
		valueMap.put(k, v); // To get "real" unsorted values for the comparator
		return super.put(k, v); // Put it in value order
	}

}
