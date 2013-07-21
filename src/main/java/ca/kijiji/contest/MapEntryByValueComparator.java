package ca.kijiji.contest;

import java.util.Comparator;
import java.util.Map.Entry;

public class MapEntryByValueComparator implements Comparator<Entry<String, Integer>> {

	// TODO: Make this generic for all comparable objects, not just Integers.
	@Override
	public int compare(Entry<String, Integer> entry1, Entry<String, Integer> entry2) {
		return entry2.getValue().compareTo(entry1.getValue());
	}
	

}
