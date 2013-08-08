/**
 * Kijiji programming contest July 2013:
 * http://kijijiblog.ca/so-you-think-you-can-code-eh/
 * 
 * Author: Yuan (yuan.java at gmail.com)
 * 
 * Licensed under Apache License v2.0
 *  
 */

package ca.kijiji.contest;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;


// sort the value of <key, value> for TreeSet.
class SetValueComparator implements Comparator<Entry<String, Integer>> {
	@Override
	public int compare(Entry<java.lang.String, java.lang.Integer> o1,
			Entry<java.lang.String, java.lang.Integer> o2) {
		
		// Since we know the key of entry is the KEY of the HashMap, to check if two entries are equal, we
		// need to check the keys. Otherwise, we compare their values.
		if (o2.getKey().equals(o1.getKey())) {
			return 0;
		}
		
		if (o2.getValue() < o1.getValue()) {
			return -1;
		}
		return 1;
	}	
}

// our own implementation of TreeMap that can be sorted by values 
public class TreeMapSortedByValue implements SortedMap<String, Integer> {

	// the Map is used to implement the methods like Map.get()
	private Map<String, Integer> map = new HashMap<String, Integer>();
	
	// the Set is used to implement the methods like Map.keySet(), Map.firstKey()
	private SortedSet<Entry<String, Integer>> set = new TreeSet<Entry<String, Integer>>(new SetValueComparator());
	
	// 
	public TreeMapSortedByValue(Map<String, Integer> map) {
		if (map == null) {
			return;
		}
		
		this.map = map;
		for (Map.Entry<String, Integer> entry : map.entrySet()) {
			set.add(entry);
		}		
	}
	
	@Override
	public void clear() {
		map.clear();
		set.clear();
	}

	@Override
	public boolean containsKey(Object arg0) {
		return map.containsKey(arg0);
	}

	@Override
	public boolean containsValue(Object arg0) {
		return map.containsValue(arg0);
	}

	@Override
	public Integer get(Object arg0) {
		return map.get(arg0);
	}

	@Override
	public boolean isEmpty() {
		return map.isEmpty();
	}

	@Override
	public Integer put(String arg0, Integer arg1) {
		Integer ret = map.put(arg0, arg1);

		// for TreeSet, remove the entry first
		if (ret != null) {
			set.remove(new AbstractMap.SimpleEntry<String, Integer>(arg0, ret));
		}
		set.add(new AbstractMap.SimpleEntry<String, Integer>(arg0, arg1));
		
		return ret;
	}

	@Override
	public void putAll(Map<? extends String, ? extends Integer> arg0) {
		map.putAll(arg0);
		
		for (Entry<? extends String, ? extends Integer> entry : arg0.entrySet()) {
			set.add((Entry<String, Integer>) entry);
		}
	}

	@Override
	public Integer remove(Object arg0) {
		Integer ret = map.remove(arg0);
		
		set.remove(new AbstractMap.SimpleEntry<String, Integer>((String) arg0, null));
		return ret;
	}

	@Override
	public int size() {
		return map.size();
	}

	@Override
	public Comparator<? super String> comparator() {
		return null;
	}

	@Override
	public Set<java.util.Map.Entry<String, Integer>> entrySet() {		
		return map.entrySet();
	}

	@Override
	public String firstKey() {		
		// return the key of the set
		return set.first().getKey();
	}

	@Override
	public SortedMap<String, Integer> headMap(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<String> keySet() {
		// return the key set in the SortedSet.
		Set<String> kSet = new LinkedHashSet<String>();
		for (Entry<String, Integer> e : set) {
			kSet.add(e.getKey());
		}		
		return kSet;
	}

	@Override
	public String lastKey() {		
		return set.last().getKey();
	}

	@Override
	public SortedMap<String, Integer> subMap(String arg0, String arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SortedMap<String, Integer> tailMap(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Collection<Integer> values() {
		// return the value set in the SortedSet.
		Set<Integer> vSet = new LinkedHashSet<Integer>();
		for (Entry<String, Integer> e : set) {
			vSet.add(e.getValue());
		}		
		return vSet;
	}

}
