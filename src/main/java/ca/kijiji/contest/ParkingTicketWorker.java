package ca.kijiji.contest;


import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import com.twitter.jsr166e.LongAdder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParkingTicketWorker extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(ParkingTicketWorker.class);

    // Splits the CSV line into separate fields, *this will not work for field with escaped commas
    // and isn't compliant with the CSV spec!* But, only 23 such fields exist in the input, so we can be lazy.
    private static final Splitter FIELD_SPLITTER = Splitter.on(',');

    public static final int ADDR_COLUMN = 7;
    public static final int FINE_COLUMN = 4;

    // Split the parts of the street name up
    private static final Splitter STREET_TOKENIZER = Splitter.on(' ').omitEmptyStrings();
    // Join the tokens back together
    private static final Joiner STREET_JOINER = Joiner.on(' ');
    // Regex to separate the street number from the street name
    // there need not be a street number, but it must be a combination of digits and punctuation with
    // an optional letter at the end for apartments. (ex. 123/345, 12451&2412, 2412a, etc.)
    private static final Pattern ADDR_REGEX =
            Pattern.compile("^[^\\p{N}\\p{L}]*(?:(?<num>[\\p{N}\\-&/, ]*(?:\\p{N}\\p{L})?)\\s+)?(?<street>[\\p{N}\\p{L} \\.'-]+).*");

    // Set of directions a street may end with
    private static final ImmutableSet<String> DIRECTION_SET = ImmutableSet.of(
            //"NS" means either North *or* South? Only shows up in a couple of places
            "N", "NORTH", "S", "SOUTH", "W", "WEST", "E", "EAST", "NE", "NW", "SW", "SE", "NS"
    );

    // Set of designators to remove from the end of street names (ST, ROAD, etc.)
    // The designation may be necessary for disambiguation, so it'd be *better* to normalize the designation,
    // but this test requires no designations, so use a hash set of designations to ignore
    public static final ImmutableSet<String> DESIGNATION_SET = ImmutableSet.of(
            // mostly from the top of
            // `cut -d, -f8 Parking_Tags_Data_2012.csv | sed 's/\s+$//g' | awk -F' ' '{print $NF}' | sort | uniq -c | sort -n`
            "AV", "AVE", "AVENUE", "BL", "BLV", "BLVD", "BOULEVARD", "CIR", "CIRCLE", "CR", "CRCL", "CRCT", "CRES", "CRS",
            "CRST", "CRESCENT", "CT", "CRT", "COURT", "D", "DR", "DRIVE", "GARDEN", "GDN", "GDNS", "GARDENS", "GR", "GRDNS",
            "GROVE", "GRV", "GT", "HGHTS", "HEIGHTS", "HTS", "HILL", "LN", "LANE", "MANOR", "MEWS", "PARKWAY", "PK", "PKWY",
            "PRK", "PL", "PLCE", "PLACE", "PROMENADE", "QUAY", "RD", "ROAD", "ST", "STR", "SQ", "SQUARE", "STREET", "T", "TER",
            "TERR", "TERRACE", "TR", "TRL", "TRAIL", "VISTA", "V", "WAY", "WY", "WOOD"

    );


    // decrement this when we leave run()
    private final CountDownLatch mRunningCounter;
    // street name -> profit map
    private final ConcurrentMap<String, LongAdder> mStreetStats;
    // Normalized name cache, makes it complete around 30% faster on my PC.
    private final ConcurrentMap<String, String> mStreetNameCache;
    // How the main thread communicates with us
    private final LinkedBlockingQueue<ParkingTicketMessage> mMessageQueue;

    public ParkingTicketWorker(CountDownLatch counter, ConcurrentMap<String, LongAdder> statsMap,
                               ConcurrentMap<String, String> nameCacheMap, LinkedBlockingQueue<ParkingTicketMessage> queue) {
        mRunningCounter = counter;
        mStreetStats = statsMap;
        mStreetNameCache = nameCacheMap;
        mMessageQueue = queue;
    }

    public void run ()  {
        while(true) {
            try {

                // Wait for a new message
                ParkingTicketMessage message = mMessageQueue.take();

                if(message.isLastMessage()) {
                    // It's the last message, rebroadcast to all the other consumers so they shut down as well.
                    mMessageQueue.put(message);
                    break;
                }

                // Split the ticket into columns
                String[] ticketCols = Iterables.toArray(FIELD_SPLITTER.split(message.getTicket()), String.class);

                // Is there even an address column we could read from?
                if(ticketCols.length <= ADDR_COLUMN) {
                    LOG.warn(String.format("Line had too few columns: %s", message.getTicket()));
                    continue;
                }

                // Get the column containing the address of the infraction
                String addr = ticketCols[ADDR_COLUMN];

                // If the address is empty, fetch the next address
                if(StringUtils.isNullOrBlank(addr)) {
                    continue;
                }

                // Get just the street name without the street number(s)
                String streetName = addrToStreetName(addr);

                // We were able to parse a street name out of the address
                if(!StringUtils.isNullOrBlank(streetName)) {
                    // Figure out how much the fine for this infraction was
                    long fine = Longs.tryParse(ticketCols[FINE_COLUMN]);

                    addFineTo(streetName, fine);
                }
                // There are plenty of funky looking addresses in the CSV, they're really not exceptional.
                // Just log whatever weirdness we get and ignore it.
                else {
                    LOG.warn(String.format("Couldn't parse address: %s", addr));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        mRunningCounter.countDown();
    }

    protected String addrToStreetName(String addr) {
        // split the address into street number and street name components
        Matcher addrMatches = ADDR_REGEX.matcher(addr.toUpperCase().trim());

        // this doesn't really look like an address...
        if(!addrMatches.matches()) {
            return null;
        }

        // Pull the full street name out of the address and check if we have the normalized name cached.
        String fullStreetName = addrMatches.group("street");
        String streetName = mStreetNameCache.get(fullStreetName);

        // No normalized version in the cache, calculate it
        if(streetName == null) {

            String[] foo = new String[20];

            // Split the street up into tokens (may contain
            ArrayList<String> streetToks = Lists.newArrayList(STREET_TOKENIZER.split(fullStreetName));

            // Go backwards through the tokens and skip all the ones that aren't likely part of the actual name.
            int lastNameElem = 0;

            for(int i = streetToks.size() - 1; i >= 0; --i) {
                String tok = streetToks.get(i);

                // There may be multiple direction tokens (N E, S E, etc.) but they never show up before a
                // street designation. Stop looking at tokens as soon as we hit the first token that looks
                // like a street designation otherwise we'll mangle names like "HILL STREET"
                lastNameElem = i;

                if(DESIGNATION_SET.contains(tok)) {
                    break;
                }
                // This is neither a direction nor a designation token, this is part of the street name!
                // Bail out.
                if(!DIRECTION_SET.contains(tok)) {
                    lastNameElem += 1;
                    break;
                }
            }


            // get the fully built street name
            streetName = STREET_JOINER.join(streetToks.subList(0, lastNameElem));

            // add this street name to the cache
            mStreetNameCache.putIfAbsent(fullStreetName, streetName);
        }

        return streetName;
    }

    protected void addFineTo(String streetName, long fine) {
        // Look for the map entry for this street's fines
        LongAdder fineTracker = mStreetStats.get(streetName);

        // Alright, couldn't find an existing fine tracker. We can't avoid locking now. Try putting one in,
        // or if someone else puts one in before us, use that.
        if(fineTracker == null) {
            final LongAdder newFineTracker = new LongAdder();
            fineTracker = mStreetStats.putIfAbsent(streetName, newFineTracker);

            // There *still* wasn't a fine tracker for this street in the map, the current tracker is the
            // one we just tried to add.
            if(fineTracker == null) {
                fineTracker = newFineTracker;
            }
        }

        // Add it to the total for this street
        fineTracker.add(fine);
    }
}
