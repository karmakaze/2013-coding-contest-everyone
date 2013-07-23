package ca.kijiji.contest;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for picking the street name out of an address entry
 * in the Toronto Parking Ticket database.
 *
 * @author Jonathan Fuerth <jfuerth@redhat.com>
 */
public class StreetAmountParser {

  private static final Logger LOG = LoggerFactory.getLogger(StreetAmountParser.class);

  /**
   * A regular expression matching the part that comes before the street name in
   * the street address. This part is the hairiest part of the data; it looks
   * like it was keyed in manually on horrible keyboards by people who don't
   * care about data quality. There are a lot of random characters in the street
   * numbers, as well as lookalikes such as lowercase l standing in for the
   * digit 1.
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

  /**
   * Parses the given line of parking ticket data into a StreetAmount instance.
   * If the street name cannot be recovered from the address, the original line
   * is logged at the DEBUG level, and a StreetAddress with
   * {@code *BAD ADDRESS*} as the street name is returned.
   *
   * @param line
   *          a line from the Toronto Parking Tickets CSV file.
   * @return a StreetAmount instance with the street name and ticket amount for
   *         the given line.
   */
  public static StreetAmount parse(String line) {
    String[] fields = line.split(",");
    int amount = Integer.parseInt(fields[4]);
    String address = fields[7];
    Matcher matcher = FULL_PATTERN.matcher(address);
    String street;
    if (matcher.matches()) {
      street = matcher.group(2);
    }
    else {
      LOG.debug("Unparseable address: {}", fields[7]);
      street = "*BAD ADDRESS*";
    }
    return new StreetAmount(amount, street);
  }

}
