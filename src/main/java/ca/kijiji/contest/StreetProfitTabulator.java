package ca.kijiji.contest;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class StreetProfitTabulator extends AbstractTicketWorker {

    private static final Logger LOG = LoggerFactory.getLogger(StreetProfitTabulator.class);

    // street name -> profit map
    private final ConcurrentHashMap<String, AtomicInteger> _mStreetStats;
    // Normalized name cache, makes it complete around 30% faster on my PC.
    private final StreetNameResolver _mStreetNameResolver;

    public StreetProfitTabulator(CountDownLatch runCounter, LinkedBlockingQueue<String> queue, AtomicInteger errCounter,
                                 ConcurrentHashMap<String, AtomicInteger> statsMap, StreetNameResolver nameCacheMap) {
        super(runCounter, errCounter, queue);
        _mStreetStats = statsMap;

        _mStreetNameResolver = nameCacheMap;
    }

    /**
     * Add the fine from this ticket to <code>_mStreetStats</code> if it references a valid street
     * @param ticketCols columns from the CSV
     */
    protected void processTicketCols(String[] ticketCols) {
        // Get the column containing the address of the infraction
        String address = getColumn(ticketCols, "location2").trim();

        // We can't do anything if there's no address, fetch the next ticket
        if(address.isEmpty()) {
            return;
        }

        // Get just the street name without the street number(s)
        String streetName = _mStreetNameResolver.addressToStreetName(address);

        // We were able to parse a street name out of the address
        if(streetName != null) {
            // Figure out how much the fine for this infraction was
            Integer fine = Ints.tryParse(getColumn(ticketCols, "set_fine_amount"));

            if(fine != null) {
                addFineTo(streetName, fine);
            } else {
                mErrCounter.getAndIncrement();
            }
        } else {
            // There are plenty of funky looking addresses in the CSV, they're really not exceptional.
            // Just make a note of whatever weirdness we get and ignore it.

            mErrCounter.getAndIncrement();

            // I don't know what it is about log4j's appenders, but printing 50 of these
            // adds 300+ ms of latency. Only print them if we're debugging.
            LOG.debug(String.format("Couldn't parse address: %s", address));
        }
    }

    /**
     * Add to the profit total for streetName
     * @param streetName street the infraction occurred on
     * @param fine fine to add to the street's total profit
     */
    protected void addFineTo(String streetName, int fine) {
        // Look for the map entry for this street's profits
        AtomicInteger profitTracker = _mStreetStats.get(streetName);

        // Alright, couldn't find an existing profit tracker. We can't avoid locking now. Try putting one in,
        // or if someone else puts one in before us, use that.
        if(profitTracker == null) {
            final AtomicInteger newProfitTracker = new AtomicInteger(0);
            profitTracker = _mStreetStats.putIfAbsent(streetName, newProfitTracker);

            // Nobody tried inserting one before we did, use the one we just inserted.
            if(profitTracker == null) {
                profitTracker = newProfitTracker;
            }
        }

        // Add it to the total for this street
        profitTracker.getAndAdd(fine);
    }
}
