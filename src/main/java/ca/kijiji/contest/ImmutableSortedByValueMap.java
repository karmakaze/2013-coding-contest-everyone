package ca.kijiji.contest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
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
	private SortedMap<String, Integer> _unsortedByValueMap = null;
	private LinkedHashMap<String, Integer> _sortedByValueMap = null;
	private List<Map.Entry<String, Integer>> _sortedByValueList = null;
	
	/**
	 * Takes in an unsorted map and sorts the map by value.
	 * @param unsortedMap The unsorted map.
	 */
	public ImmutableSortedByValueMap(SortedMap<String, Integer> unsortedMap) {
		this._unsortedByValueMap = unsortedMap;
		this._sortedByValueList = new ArrayList<Map.Entry<String, Integer>>(_unsortedByValueMap.entrySet());
		MapEntryByValueComparator byValueComparator = new MapEntryByValueComparator();
		Collections.sort(this._sortedByValueList, byValueComparator);	
		this._sortedByValueMap = new LinkedHashMap<String, Integer>();
		for (Map.Entry<String, Integer> entry : this._sortedByValueList) {
			this._sortedByValueMap.put(entry.getKey(), entry.getValue());
		}			
	}
	
	@Override
	public void clear() {
		this._unsortedByValueMap.clear();
		this._sortedByValueMap.clear();
	}

	@Override
	public boolean containsKey(Object key) {
		return this._unsortedByValueMap.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		return this._sortedByValueMap.containsValue(value);
	}

	@Override
	public Integer get(Object key) {
		return this._unsortedByValueMap.get(key);
	}

	@Override
	public boolean isEmpty() {
		return this._unsortedByValueMap.isEmpty();
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
		return this._unsortedByValueMap.size();
	}

	@Override
	public Comparator<? super String> comparator() {
		throw new UnsupportedOperationException("Unimplemented.");
	}

	@Override
	public Set<java.util.Map.Entry<String, Integer>> entrySet() {
		return this._sortedByValueMap.entrySet();
	}

	@Override
	public String firstKey() {
		for (Map.Entry<String, Integer> entry : this._sortedByValueMap.entrySet()) {
			return entry.getKey();
		}
		return null;
	}

	@Override
	public SortedMap<String, Integer> headMap(String endKey) {		
		throw new UnsupportedOperationException("Unimplemented.");
	}

	@Override
	public Set<String> keySet() {
		return this._sortedByValueMap.keySet();
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
		return this._sortedByValueMap.values();
	}
}
