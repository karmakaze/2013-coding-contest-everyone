package ca.kijiji.contest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * An immutable map that is sorted by values rather than keys.
 */
public class ImmutableSortedByValueMap extends TreeMap<String, Integer> implements SortedMap<String, Integer> {

	private static final long serialVersionUID = 2250896562281350467L;
	private SortedMap<String, Integer> _unsortedMap = null;
	private List<Map.Entry<String, Integer>> _sortedByValueList = null;
	private MapEntryByValueComparator _byValueComparator = null;
	
	/**
	 * Takes in an unsorted map and sorts the map by value.
	 * @param unsortedMap The unsorted map.
	 */
	public ImmutableSortedByValueMap(SortedMap<String, Integer> unsortedMap) {
		this._unsortedMap = unsortedMap;
		this._sortedByValueList = new ArrayList<Map.Entry<String, Integer>>(_unsortedMap.entrySet());
		this._byValueComparator = new MapEntryByValueComparator();
		Collections.sort(this._sortedByValueList, _byValueComparator);		
	}
	
	@Override
	public void clear() {
		this._unsortedMap.clear();
		this._sortedByValueList.clear();
	}

	@Override
	public boolean containsKey(Object key) {
		return this._unsortedMap.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		return this._unsortedMap.containsValue(value);
	}

	@Override
	public Integer get(Object key) {
		return this._unsortedMap.get(key);
	}

	@Override
	public boolean isEmpty() {
		return this._unsortedMap.isEmpty();
	}

	@Override
	public Integer put(String key, Integer value) {
		throw new UnsupportedOperationException("Immutable.");
	}

	@Override
	public void putAll(Map<? extends String, ? extends Integer> m) {
		throw new UnsupportedOperationException("Immutable.");
	}

	@Override
	public Integer remove(Object key) {
		throw new UnsupportedOperationException("Immutable.");
	}

	@Override
	public int size() {
		return this._sortedByValueList.size();
	}

	@Override
	public Comparator<? super String> comparator() {
		// Unimplemented.
		throw new UnsupportedOperationException("Unimplemented.");
	}

	@SuppressWarnings("unchecked")
	@Override
	public Set<java.util.Map.Entry<String, Integer>> entrySet() {
		return (Set<Map.Entry<String, Integer>>) this._sortedByValueList;
	}

	@Override
	public String firstKey() {
		return this._sortedByValueList.get(0).getKey();
	}

	@Override
	public SortedMap<String, Integer> headMap(String endKey) {
		throw new UnsupportedOperationException("Unimplemented.");
	}

	@Override
	public Set<String> keySet() {
		Set<String> keys = new HashSet<String>();
		for (Map.Entry<String, Integer> entries : this._sortedByValueList) {
			keys.add(entries.getKey());
		}
		return keys;
	}

	@Override
	public String lastKey() {
		return this._sortedByValueList.get(this._sortedByValueList.size() - 1).getKey();
	}

	@Override
	public SortedMap<String, Integer> subMap(String arg0, String arg1) {
		throw new UnsupportedOperationException("Unimplemented.");
	}

	@Override
	public SortedMap<String, Integer> tailMap(String arg0) {
		throw new UnsupportedOperationException("Unimplemented.");
	}

	@Override
	public Collection<Integer> values() {
		return this._unsortedMap.values();
	}
}
