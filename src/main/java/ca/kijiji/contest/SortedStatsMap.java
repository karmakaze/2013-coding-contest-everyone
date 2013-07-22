package ca.kijiji.contest;

import com.google.common.base.Function;
import com.google.common.collect.Ordering;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class SortedStatsMap implements SortedMap<String, Integer> {

    private final Map<String, AtomicInteger> mInnerMap;

    private boolean mSortDirty;
    private List<String> mSortOrder = new ArrayList<>();

    // How much to multiply the stat by if we skipped tickets
    private final int mMultiplier;

    // Order entries by value, descending.
    private static final Ordering<Map.Entry<String, AtomicInteger>> ENTRY_ORDERING = Ordering.natural()
            .onResultOf(new Function<Entry<String, AtomicInteger>, Integer>() {
                public Integer apply(Map.Entry<String, AtomicInteger> entry) {
                    return entry.getValue().intValue();
                }
            }).reverse();

    public SortedStatsMap(Map<String, AtomicInteger> statsMap, int multiplier) {
        mInnerMap = statsMap;
        mMultiplier = multiplier;
        mSortDirty = true;
        _updateSortOrder();
    }

    @Override
    public String firstKey() {
        _updateSortOrder();
        if(mSortOrder.size() == 0)
            return null;
        return mSortOrder.get(0);
    }

    @Override
    public String lastKey() {
        _updateSortOrder();
        if(mSortOrder.size() == 0)
            return null;
        return mSortOrder.get(mSortOrder.size() - 1);
    }

    @Override
    public Integer get(Object key) {
        // Return our best guess of what the fine total should be based
        // on the multiplier
        return _getApproxValue(mInnerMap.get(key));
    }

    private void _updateSortOrder() {
        if(mSortDirty) {

            mSortOrder.clear();

            // Get a sorted version of the map's entries
            List<Entry<String, AtomicInteger>> sortedSets =
                    ENTRY_ORDERING.sortedCopy(mInnerMap.entrySet());

            // We only care about the first 100 elements (for speed)
            Iterator<Entry<String, AtomicInteger>> listIter = sortedSets.listIterator();
            Iterator<Entry<String, AtomicInteger>> listEndIter = sortedSets.listIterator(100);

            while(listIter.hasNext() && !listIter.equals(listEndIter)) {
                mSortOrder.add(listIter.next().getKey());
            }

            mSortDirty = false;
        }
    }

    private Integer _getApproxValue(AtomicInteger atomInt) {
        return atomInt.intValue() * mMultiplier;
    }

    // Methods that we're obligated to pretend to support

    @Override
    public Comparator<? super String> comparator() {
        // We don't compare keys for sorting, so we can't return a comparator
        throw new UnsupportedOperationException();
    }

    @Override
    public SortedMap<String, Integer> subMap(String s, String s2) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SortedMap<String, Integer> headMap(String s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SortedMap<String, Integer> tailMap(String s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        return mInnerMap.size();
    }

    @Override
    public boolean isEmpty() {
        return mInnerMap.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return mInnerMap.containsKey(key);
    }

    @Override
    public boolean containsValue(Object val) {
        // We don't have exact value mappings
        // since we use AtomicIntegers and a multiplier...
        throw new UnsupportedOperationException();
    }

    @Override
    public Integer put(String key, Integer val) {
        // This makes inserts expensive, so, uh, don't insert.
        mSortDirty = true;
        return _getApproxValue(mInnerMap.put(key, new AtomicInteger(val)));
    }

    @Override
    public Integer remove(Object key) {
        mSortDirty = true;
        return _getApproxValue(mInnerMap.remove(key));
    }

    @Override
    public void putAll(Map<? extends String, ? extends Integer> map) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> keySet() {
        return null;
    }

    @Override
    public Collection<Integer> values() {
        return null;
    }

    @Override
    public Set<Entry<String, Integer>> entrySet() {
        return null;
    }
}
