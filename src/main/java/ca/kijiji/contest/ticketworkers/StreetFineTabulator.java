package ca.kijiji.contest.ticketworkers;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import ca.kijiji.contest.StreetNameResolver;
import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreetFineTabulator extends AbstractTicketWorker {

    private static final Logger LOG = LoggerFactory.getLogger(StreetFineTabulator.class);

    // street name -> profit map
    private final ConcurrentHashMap<String, AtomicInteger> _mStreetStats;
    // Normalized name cache, makes it complete around 30% faster on my PC.
    private final StreetNameResolver _mStreetNameResolver;

    public StreetFineTabulator(CountDownLatch counter, LinkedBlockingQueue<String> queue,
                               ConcurrentHashMap<String, AtomicInteger> statsMap, StreetNameResolver nameCacheMap) {
        super(counter, queue);
        _mStreetStats = statsMap;
        _mStreetNameResolver = nameCacheMap;
    }

    protected void processTicketCols(String[] ticketCols) {
        // Get the column containing the address of the infraction
        String addr = ticketCols[ADDR_COLUMN].trim();

        // We can't do anything if there's no address, fetch the next ticket
        if(addr.isEmpty()) {
            return;
        }

        // Get just the street name without the street number(s)
        String streetName = _mStreetNameResolver.addrToStreetName(addr);

        // We were able to parse a street name out of the address
        if(streetName != null && !streetName.trim().isEmpty()) {
            // Figure out how much the fine for this infraction was
            Integer fine = Ints.tryParse(ticketCols[FINE_COLUMN]);

            if(fine != null)
                addFineTo(streetName, fine);
        }
        // There are plenty of funky looking addresses in the CSV, they're really not exceptional.
        // Just log whatever weirdness we get and ignore it.
        else {
            LOG.warn(String.format("Couldn't parse address: %s", addr));
        }
    }

    /**
     * Add to the total fine for <code>streetName</code>
     * @param streetName street the infraction occurred on
     * @param fine fine to add to the street total
     */
    protected void addFineTo(String streetName, int fine) {
        // Look for the map entry for this street's fines
        AtomicInteger fineTracker = _mStreetStats.get(streetName);

        // Alright, couldn't find an existing fine tracker. We can't avoid locking now. Try putting one in,
        // or if someone else puts one in before us, use that.
        if(fineTracker == null) {
            final AtomicInteger newFineTracker = new AtomicInteger();
            fineTracker = _mStreetStats.putIfAbsent(streetName, newFineTracker);

            // Nobody tried inserting one before we did, use the one we just inserted.
            if(fineTracker == null) {
                fineTracker = newFineTracker;
            }
        }

        // Add it to the total for this street
        fineTracker.getAndAdd(fine);
    }
}
