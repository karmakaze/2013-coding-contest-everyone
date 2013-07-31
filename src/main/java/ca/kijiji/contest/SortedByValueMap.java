package ca.kijiji.contest;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;

/**
 * A bastardized implementation of the SortedMap interface which supports lookup
 * by key as usual, but iterates over its entries by order of their value, from
 * highest to lowest. <b>The SortedMap interface specifies iteration order is by
 * key, and this implementation violates that contract.</b>
 *
 * @author Jonathan Fuerth <jfuerth@gmail.com>
 */
public class SortedByValueMap<K, V extends Comparable<V>> extends AbstractMap<K, V> implements SortedMap<K, V> {

  /**
   * Implements the SortedSet interface by providing the elements of a list in
   * that list's iteration order. The list is assumed to be pre-sorted according to
   * whatever criteria are important to the user. The {@code add()} operation is
   * not supported, so the set should remain sorted as long as the items it
   * contains are not modified in place.
   *
   * @author Jonathan Fuerth <jfuerth@redhat.com>
   */
  private final class OrderedSet<E> extends AbstractSet<E> implements SortedSet<E> {

    private final List<E> elements;

    public OrderedSet(List<E> elements) {
      this.elements = elements;
    }

    @Override
    public Iterator<E> iterator() {
      return elements.iterator();
    }

    @Override
    public int size() {
      return elements.size();
    }

    public Comparator<? super E> comparator() {
      return null;
    }

    public OrderedSet<E> subSet(E fromElement, E toElement) {
      return new OrderedSet<E>(elements.subList(elements.indexOf(fromElement), elements.indexOf(toElement)));
    }

    public OrderedSet<E> headSet(E toElement) {
      return new OrderedSet<E>(elements.subList(0, elements.indexOf(toElement)));
    }

    public OrderedSet<E> tailSet(E fromElement) {
      return new OrderedSet<E>(elements.subList(elements.indexOf(fromElement), elements.size()));
    }

    public E first() {
      return elements.get(0);
    }

    public E last() {
      return elements.get(elements.size() - 1);
    }

  }

  private final OrderedSet<Map.Entry<K, V>> entrySet;

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
    List<Map.Entry<K, V>> sortedEntries = new ArrayList<Map.Entry<K,V>>(entries);
    Collections.sort(sortedEntries, new Comparator<Map.Entry<K,V>>() {
      public int compare(java.util.Map.Entry<K, V> o1, java.util.Map.Entry<K, V> o2) {
        return o2.getValue().compareTo(o1.getValue());
      }
    });
    this.entrySet = new OrderedSet<Map.Entry<K,V>>(sortedEntries);
  }

  private SortedByValueMap(OrderedSet<Map.Entry<K, V>> backingSet) {
    entrySet = backingSet;
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
    Iterator<Map.Entry<K, V>> it = entrySet.iterator();
    Map.Entry<K, V> fromElement = null;
    while (it.hasNext() && fromElement == null) {
      Map.Entry<K, V> entry = it.next();
      if (entry.getKey().equals(fromKey)) {
        fromElement = entry;
      }
    }
    Map.Entry<K, V> toElement = null;
    while (it.hasNext() && toElement == null) {
      Map.Entry<K, V> entry = it.next();
      if (entry.getKey().equals(toKey)) {
        toElement = entry;
      }
    }
    if (fromElement == null || toElement == null) {
      throw new IllegalArgumentException("SortedByValueMap requires exact key matches for submaps");
    }
    return new SortedByValueMap<K, V>(entrySet.subSet(fromElement, toElement));
  }

  public SortedMap<K, V> headMap(K toKey) {
    Iterator<Map.Entry<K, V>> it = entrySet.iterator();
    Map.Entry<K, V> toElement = null;
    while (it.hasNext() && toElement == null) {
      Map.Entry<K, V> entry = it.next();
      if (entry.getKey().equals(toKey)) {
        toElement = entry;
      }
    }
    if (toElement == null) {
      throw new IllegalArgumentException("SortedByValueMap requires exact key matches for submaps");
    }
    return new SortedByValueMap<K, V>(entrySet.headSet(toElement));
  }

  public SortedMap<K, V> tailMap(K fromKey) {
    Iterator<Map.Entry<K, V>> it = entrySet.iterator();
    Map.Entry<K, V> fromElement = null;
    while (it.hasNext() && fromElement == null) {
      Map.Entry<K, V> entry = it.next();
      if (entry.getKey().equals(fromKey)) {
        fromElement = entry;
      }
    }
    if (fromElement == null) {
      throw new IllegalArgumentException("SortedByValueMap requires exact key matches for submaps");
    }
    return new SortedByValueMap<K, V>(entrySet.tailSet(fromElement));
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