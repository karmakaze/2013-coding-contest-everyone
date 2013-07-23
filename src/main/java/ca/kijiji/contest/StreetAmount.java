package ca.kijiji.contest;

import java.util.Map;

/**
 * A mutable map entry implementation that accumulates the total dollar value of
 * fines for a specific street.
 *
 * @author Jonathan Fuerth <jfuerth@redhat.com>
 */
public final class StreetAmount implements Map.Entry<String, Integer> {

  private final String street;
  private int amount;

  public StreetAmount(int amount, String location) {
    this.amount = amount;

    if (location == null) {
      throw new NullPointerException();
    }
    this.street = location;
  }

  @Override
  public String toString() {
    return street + ": $" + amount;
  }

  public String getKey() {
    return street;
  }

  public Integer getValue() {
    return amount;
  }

  public Integer setValue(Integer value) {
    Integer oldValue = amount;
    this.amount = value;
    return oldValue;
  }

  /**
   * Merges the value of the given StreetAmount into this one. If assertions are
   * turned on in the JVM, the given amount is checked to see if the street name
   * matches.
   *
   * @param amount2
   */
  public void merge(StreetAmount mergeMe) {
    amount += mergeMe.amount;
  }
}
