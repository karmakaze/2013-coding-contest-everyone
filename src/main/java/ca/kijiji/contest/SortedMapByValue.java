package ca.kijiji.contest;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Comparator;
import java.util.Set;
import java.util.SortedMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

//Based on [TreeMap sort by value](http://stackoverflow.com/questions/2864840/treemap-sort-by-value).

public class SortedMapByValue<K,V extends Comparable<V>> extends AbstractMap<K,V> implements SortedMap<K,V> {
	
	private SortedSet<Map.Entry<K,V>> _entrySet;
	
	public SortedMapByValue() {
		super();
		
		_entrySet = new TreeSet<Map.Entry<K,V>>(
			new Comparator<Map.Entry<K,V>>() {
				@Override
				public int compare(Map.Entry<K,V> e1, Map.Entry<K,V> e2) {
					return e2.getValue().compareTo(e1.getValue());
				}
			}
		);
	}
	
	// SortedMap Interface
	
	public Comparator<? super K> comparator() {
		return null;
	}
	
	public SortedSet<Map.Entry<K,V>> entrySet() {
		return _entrySet;
	}
	
	public K firstKey() {
		return _entrySet.first().getKey();
	}
	
	public SortedMap<K,V> headMap(K toKey) {
		return null;
	}
	
	public Set<K> keySet() {
		return null;
	}
	
	public K lastKey() {
		return _entrySet.last().getKey();
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
