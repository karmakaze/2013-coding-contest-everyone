package ca.kijiji.contest;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class StreetProfitTabulator extends AbstractTicketWorker {

    private static final Logger LOG = LoggerFactory.getLogger(StreetProfitTabulator.class);

    // street name -> profit map
    private final StreetProfitMap _mStreetStats;
    // Normalized name cache, makes it complete around 30% faster on my PC.
    private final StreetNameResolver _mStreetNameResolver;

    public StreetProfitTabulator(CountDownLatch runCounter, LinkedBlockingQueue<CharRange> queue, AtomicInteger errCounter,
                                 StreetProfitMap statsMap, StreetNameResolver nameCacheMap) {
        super(runCounter, errCounter, queue);
        _mStreetStats = statsMap;

        _mStreetNameResolver = nameCacheMap;
    }

    /**
     * Add the fine from this ticket to <code>_mStreetStats</code> if it references a valid street
     * @param ticketCols columns from the CSV
     */
    protected void processTicketCols(List<CharRange> ticketCols) {
        // Get the column containing the address of the infraction
        CharRange address = getColumn(ticketCols, "location2");
        address.trim();

        // We can't do anything if there's no address, fetch the next ticket
        if(address.isEmpty()) {
            return;
        }

        // Get just the street name without the street number(s)
        String streetName = _mStreetNameResolver.addressToStreetName(address);

        // We were able to parse a street name out of the address
        if(streetName != null) {
            // Figure out how much the fine for this infraction was
            CharRange fineField = getColumn(ticketCols, "set_fine_amount");
            Integer fine = fineField.toInteger();

            if(fine != null) {
                _mStreetStats.addFineTo(streetName, fine);
            } else {
                // Welp, looks like there was something weird in the fine field.
                LOG.warn(String.format("%s is not a valid value for the fine field", fineField));
                mErrCounter.getAndIncrement();
            }
        } else {
            // There are plenty of funky looking addresses in the CSV, they're really not exceptional.
            // Just make a note of whatever weirdness we get.

            mErrCounter.getAndIncrement();

            // I don't know what it is about log4j's appenders, but printing 50 of these
            // adds a good amount of latency. Still, it's probably more important to let people
            // know that their data's all jacked up.
            LOG.warn(String.format("Couldn't parse address: %s", address));
        }
    }
}
