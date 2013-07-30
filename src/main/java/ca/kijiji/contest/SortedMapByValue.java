package ca.kijiji.contest;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Comparator;
import java.util.Set;
import java.util.SortedMap;
import java.util.Map;

public class SortedMapByValue<K,V> extends AbstractMap<K,V> implements SortedMap<K,V> {
	
	// Based on [TreeMap sort by value](http://stackoverflow.com/questions/2864840/treemap-sort-by-value).
	
	public SortedMapByValue() {
		super();
	}
	
	// SortedMap Interface
	
	public Comparator<? super K> comparator() {
		return null;
	}
	
	public Set<Map.Entry<K,V>> entrySet() {
		return null;
	}
	
	public K firstKey() {
		return null;
	}
	
	public SortedMap<K,V> headMap(K toKey) {
		return null;
	}
	
	public Set<K> keySet() {
		return null;
	}
	
	public K lastKey() {
		return null;
	}
	
	public SortedMap<K,V> subMap(K fromKey, K toKey) {
		return null;
	}
	
	public SortedMap<K,V> tailMap(K fromKey) {
		return null;
	}
	
	public Collection<V> values() {
		return null;
	}

}
