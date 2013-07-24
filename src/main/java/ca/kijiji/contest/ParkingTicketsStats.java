package ca.kijiji.contest;

import java.io.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import org.apache.commons.lang.StringUtils;
import org.slf4j.*;

import ca.kijiji.contest.ticketworkers.*;

// The input is pretty dirty (that nasty business wasn't a joke!) so you can expect things like "YONGE STRET",
// "PENGARTH CROUT" and "BEVERLEY ST BLOCKING PRIVATE DRWY" that would normally be fixed with a manual once-over...
// but let's pretend we have good data, These errors are small enough not to cause huge problems

// Don't park anywhere near Seneca college.

public class ParkingTicketsStats {

    private static final Logger LOG = LoggerFactory.getLogger(ParkingTicketsStats.class);

    // How many pending messages can be in the queue before insertions block
    private static final int MSG_QUEUE_SIZE = 4096;

    /**
     * Tabulates the total value of the parking tickets issued per street with no fuzzing applied
     * @param parkingTicketsStream Stream containing the CSV with the tickets
     * @return A sorted map of Street Name -> Total Profit
     * @throws IOException
     * @throws InterruptedException
     */
    public static SortedMap<String, Integer> sortStreetsByProfitability(InputStream parkingTicketsStream)
            throws IOException, InterruptedException {

        return sortStreetsByProfitability(parkingTicketsStream, 0);
    }

    /**
     * Tabulates the total value of the parking tickets issued per street
     * @param parkingTicketsStream Stream containing the CSV with the tickets
     * @param numSkipTickets How many tickets to skip for each one read, makes processing faster but less accurate.
     *                       The total profit is adjusted to arrive at our best guess without reading every ticket.
     *                       The appropriate value depends on your use-case, but may be unreliable past 6.
     * @return A sorted map of Street Name -> Total Profit
     * @throws IOException
     * @throws InterruptedException
     */
    public static SortedMap<String, Integer> sortStreetsByProfitability(InputStream parkingTicketsStream, int numSkipTickets)
            throws IOException, InterruptedException {

        BufferedReader parkingCsvReader = new BufferedReader(new InputStreamReader(parkingTicketsStream));

        // Normalized name cache, makes it complete around 30% faster on my PC.
        StreetNameResolver streetNameResolver = new StreetNameResolver();
        // Map of street name -> total profit
        ConcurrentHashMap<String, AtomicInteger> stats = new ConcurrentHashMap<>();

        // Use as many workers as we have cores - 1 up to a maximum of 3 workers, but always use at least one.
        int numWorkerThreads = Math.max(Math.min(Runtime.getRuntime().availableProcessors() - 1, 3), 1);

        // A single worker thread is fastest when the main thread is wasting its time skipping tickets anyways.
        if(numSkipTickets > 2)
            numWorkerThreads = 1;

        // Set up communication with the threads
        LinkedBlockingQueue<String> messageQueue = new LinkedBlockingQueue<>(MSG_QUEUE_SIZE);
        CountDownLatch countDownLatch = new CountDownLatch(numWorkerThreads);

        // Count the number of bogus addresses we hit
        AtomicInteger errCounter = new AtomicInteger(0);


        // Parse out the column header
        String[] csvCols = StringUtils.splitPreserveAllTokens(parkingCsvReader.readLine(), ',');

        // Dispatch using a ThreadPool and Runnable for each entry was 2 times slower than manually
        // handling task dispatch, slower than the non-threaded version! Manually manage work dispatch
        // with long-running threads.

        // Set up the worker threads
        for(int i = 0; i < numWorkerThreads; ++i) {
            AbstractTicketWorker worker =
                    new StreetProfitTabulator(countDownLatch, messageQueue, errCounter, stats, streetNameResolver);
            worker.setColumns(csvCols);
            worker.start();
        }

        // Keep sending lines to workers til we hit EOF. It's not valid to read CSVs
        // this way according to the spec, but none of the columns contain escaped newlines.
        String parkingTicketLine;
        while((parkingTicketLine = parkingCsvReader.readLine()) != null) {
            messageQueue.put(parkingTicketLine);

            // Skip by tickets we don't need to read, this doesn't handle clusters smaller than
            // numSkipTickets well and assumes relatively normal distribution.
            for(int i=0; i < numSkipTickets; ++i) {
                parkingCsvReader.readLine();
            }
        }

        // Tell the worker threads we have nothing left
        messageQueue.put(StreetProfitTabulator.END_MSG);

        // Wait for the workers to finish
        countDownLatch.await();

        // If there were any errors, print how many
        int numErrs = errCounter.get();
        if(numErrs > 0) {
            LOG.warn(String.format("Encountered %d errors during processing", numErrs));
        }

        LOG.info(String.format("%d cache hits for street name lookups", streetNameResolver.getCacheHits()));

        // Return an immutable map of the stats sorted by value
        return _finalizeStatsMap(stats, numSkipTickets + 1);
    }

    protected static SortedMap<String, Integer> _finalizeStatsMap(Map<String, AtomicInteger> stats, int multiplier) {

        // Order by profit, descending
        Ordering<Map.Entry<String, AtomicInteger>> entryOrdering = Ordering.natural()
                .onResultOf(new Function<Map.Entry<String, AtomicInteger>, Integer>() {
                    public Integer apply(Map.Entry<String, AtomicInteger> entry) {
                        return entry.getValue().intValue();
                    }
                }).reverse();

        // Convert the map's entries to an array
        Map.Entry<String, AtomicInteger>[] sortedStatsSet =
                (Map.Entry<String, AtomicInteger>[])Iterables.toArray(stats.entrySet(), Map.Entry.class);

        // Sort the array by the total profit for each entry
        Arrays.sort(sortedStatsSet, entryOrdering);

        // Put the keys in sortedKeyOrder in order of the value of their associated entry
        List<String> sortedKeyOrder = new ArrayList<>(sortedStatsSet.length);

        for (Map.Entry<String, AtomicInteger> entry : sortedStatsSet) {
            sortedKeyOrder.add(entry.getKey());
        }

        // Put the results into an immutable map ordered by value
        ImmutableSortedMap.Builder<String, Integer> builder =
                new ImmutableSortedMap.Builder<>(Ordering.explicit(sortedKeyOrder));

        // Convert the totals to normal ints and apply the multiplier to the totals
        // to arrive at our best guess of the total profit for each street
        for (Map.Entry<String, AtomicInteger> entry : sortedStatsSet) {
            builder.put(entry.getKey(), entry.getValue().intValue() * multiplier);
        }

        return builder.build();
    }
}