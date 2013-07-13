package ca.kijiji.contest;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class StreetAmount implements Map.Entry<String, Integer> {

  /**
   * The part that comes before the street name in the street address. This part
   * is the hairiest part of the data; it looks like it was keyed in manually on
   * horrible keyboards by people who don't care about data quality. There are a
   * lot of random characters in the street numbers, as well as lookalikes such
   * as lowercase l standing in for the digit 1.
   */
  private static final String ADDRESS_PATTERN = "l?[*\"\\(\\)\\[\\]\\\\@:'=;0-9.!?%/#abc$_ -]";

  /**
   * Pattern for matching street names. This part of the data is quite clean and regular.
   * It looks like it was entered by choosing from a list of official street names.
   */
  private static final String STREET_NAME_PATTERN = "[a-zA-Z]+?[a-zA-Z0-9 '-]+";

  /**
   * All Toronto street types according to the readme file in the
   * <a href="http://opendata.toronto.ca/gcc/centreline_wgs84.zip">Toronto Centreline dataset</a>.
   * <p>
   * Note: I added the following nonstandard abbreviations because they were common in
   * the parking ticket data set:
   * <ul>
   *  <li>AV
   *  <li>CIR
   * </ul>
   */
  private static final String STREET_TYPES_PATTERN =
          "(AVENUE|AVE?|BRIDGE|BDGE|BOULEVARD|BLVD|CIRC?L?E?|CRCL|COURT|CRT|CIRCUIT|CRCT|CRESENT|CRES|" +
          "CLOSE|CS|DRIVE|DR|EXPRESSWAY|XWY|GARDEN|GDN|GARDENS|GDNS|GATE|GT|GREEN|GRN|GROVE|GRV|" +
          "HEIGHTS|HTS|HIGHWAY|HWY|HILL|HILL|LANE|LANE|LAWN|LWN|LINE|LINE|MEWS|MEWS|PARK|PK|" +
          "PARKWAY|PKWY|PATH|PATH|PATHWAY|PTWY|PLACE|PL|PROMENADE|PROM|RAMP|RAMP|ROAD|RD|ROADWAY|RDWY|" +
          "SQUARE|SQ|STREET|ST|TERRACE|TER|TRAIL|TRL|VIEW|VIEW|WALK|WALK|WAY|WAY|WOODS|WDS)";

  private static final Pattern FULL_PATTERN = Pattern.compile(
          "(" + ADDRESS_PATTERN + ")*" +        // optional "numeric" address
          " *(" + STREET_NAME_PATTERN + "?)" +  // the street name itself (this is what we're trying to capture)
          "( " + STREET_TYPES_PATTERN + ")?" +  // the street type. this really helps anchor the street names
          "( [NSEW].*)?" +                      // optional street direction
          "\\.?" +                              // some entries end with a helpful '.'
          "( UNIT #[0-9]+)?");                  // about 50 entries include an apartment number

  private int amount;
  private final String street;

  public StreetAmount(int amount, String location) {
    this.amount = amount;

    if (location == null) {
      throw new NullPointerException();
    }
    this.street = location;
  }

  /**
   * Creates a StreetAmount instance by parsing the given line of parking ticket data.
   *
   * @param line
   * @return
   */
  public static StreetAmount from(String line) {
    String[] fields = line.split(",");
    int amount = Integer.parseInt(fields[4]);
    String address = fields[7];
    Matcher matcher = FULL_PATTERN.matcher(address);
    if (matcher.matches()) {
      String street = matcher.group(2);
      return new StreetAmount(amount, street);
    }
    else {
      throw new IllegalArgumentException("Bad address: " + address);
    }
  }

  @Override
  public String toString() {
    return street + ": $" + amount;
  }

  // --- Map.Entry Implementation ---

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
}
