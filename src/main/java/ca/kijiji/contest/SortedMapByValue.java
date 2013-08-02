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
					int comparison = e2.getValue().compareTo(e1.getValue());
					
					if (comparison == 0) {
						System.out.println(e2.getKey() + " and " + e1.getKey() + " = " + e2.getValue());
						return ((String)e2.getKey()).compareTo((String)e1.getKey());
					} else {
						return comparison;
					}
				}
			}
		);
	}
	
	// SortedMap Interface
	
	public Comparator<? super K> comparator() {
		throw new UnsupportedOperationException();
	}
	
	public SortedSet<Map.Entry<K,V>> entrySet() {
		return _entrySet;
	}
	
	public K firstKey() {
		return _entrySet.first().getKey();
	}
	
	public SortedMap<K,V> headMap(K toKey) {
		throw new UnsupportedOperationException();
	}
	
	public Set<K> keySet() {
		throw new UnsupportedOperationException();
	}
	
	public K lastKey() {
		return _entrySet.last().getKey();
	}
	
	public SortedMap<K,V> subMap(K fromKey, K toKey) {
		throw new UnsupportedOperationException();
	}
	
	public SortedMap<K,V> tailMap(K fromKey) {
		throw new UnsupportedOperationException();
	}
	
	public Collection<V> values() {
		throw new UnsupportedOperationException();
	}

}
