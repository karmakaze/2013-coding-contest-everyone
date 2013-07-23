package ca.kijiji.contest;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * A bastardized implementation of the SortedMap interface which supports lookup
 * by key as usual, but iterates over its entries by order of their value, from
 * highest to lowest. <b>The SortedMap interface specifies iteration order is by
 * key, and this implementation violates that contract.</b>
 *
 * @author Jonathan Fuerth <jfuerth@gmail.com>
 */
public class SortedByValueMap<K, V extends Comparable<V>> extends AbstractMap<K, V> implements SortedMap<K, V> {

  private SortedSet<Map.Entry<K, V>> entrySet;

  /**
   * Creates a new sorted-by-value map from the given collection of map entry
   * objects. This map copies and sorts the given entries, so subsequent
   * structural modification to the given collection will not affect this map's
   * contents. However, modification to the keys and values of the entries
   * themselves will affect this map. In particular, modifications to the values
   * in the given {@code Map.Entry} objects will likely cause this map's
   * ordering to become incorrect.
   *
   * @param entries
   *          The entries this map should have. Must not be null.
   */
  public SortedByValueMap(Collection<Map.Entry<K, V>> entries) {
    this.entrySet = new TreeSet<Map.Entry<K,V>>(new Comparator<Map.Entry<K,V>>() {
      public int compare(java.util.Map.Entry<K, V> o1, java.util.Map.Entry<K, V> o2) {
        return o2.getValue().compareTo(o1.getValue());
      }
    });
    this.entrySet.addAll(entries);
  }

  /**
   * Always returns null. Maps of this type are sorted by value, highest to
   * lowest, and such a comparator is incompatible with this method's generic
   * signature.
   */
  public Comparator<? super K> comparator() {
    return null;
  }

  public SortedMap<K, V> subMap(K fromKey, K toKey) {
    throw new UnsupportedOperationException();
  }

  public SortedMap<K, V> headMap(K toKey) {
    throw new UnsupportedOperationException();
  }

  public SortedMap<K, V> tailMap(K fromKey) {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns the key that maps to the largest value in this map.
   */
  public K firstKey() {
    return entrySet.first().getKey();
  }

  /**
   * Returns the key that maps to the smallest value in this map.
   */
  public K lastKey() {
    return entrySet.last().getKey();
  }

  /**
   * Returns the entries that make up this map. The set is ordered by value,
   * highest to lowest.
   */
  @Override
  public SortedSet<Map.Entry<K, V>> entrySet() {
    return entrySet;
  }

}