package ca.kijiji.contest;


import java.util.Arrays;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Longs;
import com.twitter.jsr166e.LongAdder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParkingTicketWorker extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(ParkingTicketWorker.class);

    // Splits the CSV line into separate fields, *this will not work for fields with escaped commas or quotes
    // and isn't compliant with the CSV spec!* But, only 23 such fields exist in the input, so we can be lazy.
    private static final Splitter FIELD_SPLITTER = Splitter.on(',');

    protected static final int ADDR_COLUMN = 7;
    protected static final int FINE_COLUMN = 4;

    // decrement this when we leave run(), means no running threads when at 0
    private final CountDownLatch _mRunningCounter;
    // street name -> profit map
    private final ConcurrentMap<String, LongAdder> _mStreetStats;
    // Normalized name cache, makes it complete around 30% faster on my PC.
    private final StreetNameResolver _mStreetNameResolver;
    // How the main thread communicates with us
    private final LinkedBlockingQueue<ParkingTicketMessage> _mMessageQueue;

    public ParkingTicketWorker(CountDownLatch counter, ConcurrentMap<String, LongAdder> statsMap,
                               StreetNameResolver nameCacheMap, LinkedBlockingQueue<ParkingTicketMessage> queue) {
        _mRunningCounter = counter;
        _mStreetStats = statsMap;
        _mStreetNameResolver = nameCacheMap;
        _mMessageQueue = queue;
    }

    public void run ()  {
        while(true) {
            try {

                // Wait for a new message
                ParkingTicketMessage message = _mMessageQueue.poll();

                // Noe message yet
                if(message == null)
                    continue;

                // It's the last message, rebroadcast to all the other consumers so they shut down as well.
                if(message == ParkingTicketMessage.END) {
                    _mMessageQueue.put(message);
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
                String addr = ticketCols[ADDR_COLUMN].trim();

                // If the address is empty, fetch the next address
                if(addr.isEmpty()) {
                    continue;
                }

                // Get just the street name without the street number(s)
                String streetName = _mStreetNameResolver.addrToStreetName(addr);

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
        _mRunningCounter.countDown();
    }

    protected void addFineTo(String streetName, long fine) {
        // Look for the map entry for this street's fines
        LongAdder fineTracker = _mStreetStats.get(streetName);

        // Alright, couldn't find an existing fine tracker. We can't avoid locking now. Try putting one in,
        // or if someone else puts one in before us, use that.
        if(fineTracker == null) {
            final LongAdder newFineTracker = new LongAdder();
            fineTracker = _mStreetStats.putIfAbsent(streetName, newFineTracker);

            // Nobody tried inserting one before we did, use the one we just inserted.
            if(fineTracker == null) {
                fineTracker = newFineTracker;
            }
        }

        // Add it to the total for this street
        fineTracker.add(fine);
    }
}
