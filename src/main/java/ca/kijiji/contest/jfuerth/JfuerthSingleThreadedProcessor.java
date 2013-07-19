package ca.kijiji.contest.jfuerth;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.kijiji.contest.IParkingTicketsStatsProcessor;
import ca.kijiji.contest.ParkingTicketsStats;

public class JfuerthSingleThreadedProcessor implements IParkingTicketsStatsProcessor
{
    // Comments on the setup of the contest:
    // 1. SortedMap is specified to be ordered by its keys, not its values. Ordering one by value is awkward and violates API users' expectations.
    // 2. Stripping off the street type (ST, RD, AVE, etc) makes the results less valid because it combines streets that should be kept separate, for example Spadina Ave and Spadina Rd.

    private static final Logger LOG = LoggerFactory.getLogger(ParkingTicketsStats.class);

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

    public SortedMap<String, Integer> sortStreetsByProfitability(InputStream parkingTicketsStream) throws IOException {
      BufferedReader in = new BufferedReader(new InputStreamReader(parkingTicketsStream, "ascii"));
      Map<String, Map.Entry<String, Integer>> ticketsByStreet = new HashMap<String, Map.Entry<String, Integer>>();

      final Pattern addressPattern = Pattern.compile(
              "(" + ADDRESS_PATTERN + ")*" +        // optional "numeric" address
              " *(" + STREET_NAME_PATTERN + "?)" +  // the street name itself (this is what we're trying to capture)
              "( " + STREET_TYPES_PATTERN + ")?" +  // the street type. this really helps anchor the street names
              "( [NSEW].*)?" +                      // optional street direction
              "\\.?" +                              // some entries end with a helpful '.'
              "( UNIT #[0-9]+)?");                  // about 50 entries include an apartment number

      // this skips the header line in the data
      in.readLine();

      int badAddressCount = 0;
      int lineCount = 0;
      for (String line = in.readLine(); line != null; line = in.readLine()) {
        String[] fields = line.split(",");
        int amount = Integer.parseInt(fields[4]);
        String address = fields[7];
        Matcher matcher = addressPattern.matcher(address);
        if (matcher.matches()) {
          String street = matcher.group(2);
          StreetAmount ticket = (StreetAmount) ticketsByStreet.get(street);
          if (ticket == null) {
            ticket = new StreetAmount(amount, street);
            ticketsByStreet.put(street, ticket);
          }
          else {
            ticket.add(amount);
          }
        }
        else {
          LOG.debug("Bad address: {}", address);
          badAddressCount++;
        }
        lineCount++;
      }
      LOG.info(badAddressCount + " bad addresses (" + (100.0 * badAddressCount / lineCount) + "%)");

      SortedMap<String, Integer> result = new SortedByValueMap<String, Integer>(ticketsByStreet.values());

      if (LOG.isDebugEnabled()) {
        for (Map.Entry<String, Integer> entry : result.entrySet()) {
          LOG.debug("{}", entry);
        }
      }

      return result;
    }
}
