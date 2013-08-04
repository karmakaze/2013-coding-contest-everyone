package ca.kijiji.contest;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.*;
import com.google.common.base.Function;
import org.slf4j.*;

// The input is pretty dirty (that nasty business wasn't a joke!) so you can expect things like "YONGE STRET",
// "PENGARTH CROUT" and "BEVERLEY ST BLOCKING PRIVATE DRWY" that would normally be fixed with a manual once-over...
// but let's pretend we have good data, These errors are small enough not to cause huge problems

// Respect to Doug Lea and Cliff Click. Don't park near Seneca college.

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

        // Normalized name cache, makes it complete around 30% faster on my PC.
        StreetNameResolver streetNameResolver = new StreetNameResolver();
        // Map of street name -> total profit
        StreetProfitMap stats = new StreetProfitMap();

        // Get the number of cores (not counting the one used by the main thread)
        int freeCores = Runtime.getRuntime().availableProcessors() - 1;

        // Use as many workers as we have free cores, up to a maximum of 3 workers, always using at least one.
        int numWorkerThreads = Math.max(Math.min(freeCores, 3), 1);

        // Set up communication with the threads
        LinkedBlockingQueue<CharRange> messageQueue = new LinkedBlockingQueue<>(MSG_QUEUE_SIZE);
        CountDownLatch countDownLatch = new CountDownLatch(numWorkerThreads);


        // Count the number of bogus addresses we hit
        AtomicInteger errCounter = new AtomicInteger(0);

        // Create a reader that reads the file in chunks that the consumers may process further.
        ChunkedBufferReader parkingCsvReader = new ChunkedBufferReader(parkingTicketsStream);

        // Get a reference to the array backing the reader to give to the consumers
        char[] buffer = parkingCsvReader.getBuffer();

        // Parse out the column header
        String[] csvCols = CSVUtils.parseCSVLine(parkingCsvReader.readLine());

        // Dispatch using a ThreadPool and Runnable for each entry was 2 times slower than manually
        // handling task dispatch, slower than the non-threaded version! Manually manage work dispatch
        // with long-running threads.

        // Set up the worker threads
        for(int i = 0; i < numWorkerThreads; ++i) {
            AbstractTicketWorker worker =
                    new StreetProfitTabulator(countDownLatch, messageQueue, errCounter, buffer, stats, streetNameResolver);
            worker.setColumns(csvCols);
            worker.start();
        }

        // Keep sending chunks to workers til we hit EOF. It's not valid to read CSVs this way
        // according to the spec, but none of the columns contain escaped newlines and none should.
        CharRange parkingTicketChunk;
        while((parkingTicketChunk = parkingCsvReader.readChunkOfLines()) != null) {
            messageQueue.put(parkingTicketChunk);
        }

        // Tell the worker threads we have nothing left
        messageQueue.put(StreetProfitTabulator.END_MSG);

        // Wait for the workers to finish
        countDownLatch.await();

        // If there were any errors, print how many
        int numErrs = errCounter.intValue();
        if(numErrs > 0) {
            LOG.warn(String.format("Encountered %d errors during processing", numErrs));
        }

        LOG.info(String.format("%d cache hits for street name lookups", streetNameResolver.getCacheHits()));

        // Return an immutable map of the stats sorted by value
        return _finalizeStatsMap(stats);
    }

    /**
     * Create a sorted and immutable copy of a map of total profit from parking tickets by street.
     * Converts from the internal value format (AtomicInteger) to Integer at the same time.
     * @param stats internal total profit map
     * @return A sorted and immutable map of profit stats
     */
    private static SortedMap<String, Integer> _finalizeStatsMap(StreetProfitMap stats) {

        // Order by profit, descending
        // This isn't as straightforward as it would normally be as I've used AtomicIntegers instead of Integers
        // and AtomicInteger doesn't implement Comparable
        Ordering<Map.Entry<String, ? extends Number>> entryOrdering = Ordering.natural()
                .onResultOf(new Function<Map.Entry<String, ? extends Number>, Integer>() {
                    public Integer apply(Map.Entry<String, ? extends Number> entry) {
                        return entry.getValue().intValue();
                    }
                }).reverse();

        // Put the keys in sortedKeyOrder in order of the value of their associated entry
        List<String> sortedKeyOrder = new ArrayList<>(stats.size());

        for (Map.Entry<String, ? extends Number> entry : entryOrdering.sortedCopy(stats.entrySet())) {
            sortedKeyOrder.add(entry.getKey());
        }

        // Put the results into an immutable map ordered by value
        // It's now my belief that this *does* conform to the interface of SortedMap as it in no way
        // guarantees natural sort order or even that the sort order be based on an *intrinsic* property
        // of the key.
        ImmutableSortedMap.Builder<String, Integer> builder =
                new ImmutableSortedMap.Builder<>(Ordering.explicit(sortedKeyOrder));

        // Convert the totals to normal ints and store them in the immutable map
        for (Map.Entry<String, ? extends Number> entry : stats.entrySet()) {
            builder.put(entry.getKey(), entry.getValue().intValue());
        }

        return builder.build();
    }
}