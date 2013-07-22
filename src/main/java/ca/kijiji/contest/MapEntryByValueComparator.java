package ca.kijiji.contest;

import java.util.Comparator;
import java.util.Map.Entry;

/**
 * The comparator to sort maps by value rather than key.
 */
public class MapEntryByValueComparator implements Comparator<Entry<String, Integer>> {

	@Override
	public int compare(Entry<String, Integer> entry1, Entry<String, Integer> entry2) {
		return entry2.getValue().compareTo(entry1.getValue());
	}
	

}
