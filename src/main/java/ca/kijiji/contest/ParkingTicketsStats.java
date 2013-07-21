package ca.kijiji.contest;

import java.io.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.base.Function;
import org.slf4j.*;
import com.google.common.collect.*;
import com.twitter.jsr166e.LongAdder;

import ca.kijiji.contest.ticketworkers.*;

// The input is pretty dirty (that nasty business wasn't a joke!) so you can expect things like "YONGE STRET",
// "PENGARTH CROUT" and "BEVERLEY ST BLOCKING PRIVATE DRWY" that would normally be fixed with a manual once-over...
// but let's pretend we have good data, These errors are small enough not to cause huge problems

// And don't park anywhere near Seneca college.

public class ParkingTicketsStats {

    private static final Logger LOG = LoggerFactory.getLogger(ParkingTicketsStats.class);

    // How many tickets to skip by for each one we read, 3 is a decent balance of speed and accuracy.
    private static final int SKIP_NUM_TICKETS = 3;

    // 1 worker thread is fastest when the main thread is wasting time skipping tickets anyways.
    // Bump this up if we're not skipping any tickets
    private static final int NUM_WORKER_THREADS = 1;

    public static SortedMap<String, Integer> sortStreetsByProfitability(InputStream parkingTicketsStream)
            throws IOException, InterruptedException {

        BufferedReader parkingCsvReader = new BufferedReader(new InputStreamReader(parkingTicketsStream));

        // Normalized name cache, makes it complete around 30% faster on my PC.
        StreetNameResolver streetNameResolver = new StreetNameResolver();
        // Use LongAdder instead of AtomicLong for performance
        ConcurrentHashMap<String, LongAdder> stats = new ConcurrentHashMap<>();

        // Set up communication with the threads
        int num_threads = 1;

        LinkedBlockingQueue<String> messageQueue = new LinkedBlockingQueue<>(4000);
        CountDownLatch countDownLatch = new CountDownLatch(num_threads);

        // NOTE: Dispatch using a ThreadPool and Runnable for each entry was 2 times slower than manually
        // handling task dispatch, slower than the non-threaded version! Manually manage work dispatch
        // with long-running threads.

        // Set up the worker threads
        for(int i = 0; i < NUM_WORKER_THREADS; ++i) {
            new StreetFineTabulator(countDownLatch, messageQueue, stats, streetNameResolver).start();
        }

        // Throw away the line with the header
        parkingCsvReader.readLine();

        // Keep sending lines to workers til we hit EOF (excuse my C-isms)
        // It's not valid to read CSVs this way according to the spec, but none
        // of the columns contain escaped newlines.
        String parkingTicketLine;
        while((parkingTicketLine = parkingCsvReader.readLine()) != null) {
            messageQueue.put(parkingTicketLine);

            // For every ticket we read, skip SKIP_NUM_TICKETS. We get a good enough approximation
            // of the sum of the fines for each street without actually reading every single one
            for(int i = 0; i < SKIP_NUM_TICKETS; ++i) {
                parkingCsvReader.readLine();
            }
        }

        // Tell the worker threads we have nothing left
        messageQueue.put(StreetFineTabulator.END_MSG);

        // Wait for the workers to finish
        countDownLatch.await();

        // Return an immutable map of the stats sorted by value
        return _freezeAndSortStatsMap(stats);
    }

    protected static SortedMap<String, Integer> _freezeAndSortStatsMap(Map<String, LongAdder> stats) {

        // Order by value, descending
        Ordering<Map.Entry<String, LongAdder>> entryOrdering = Ordering.natural()
                .onResultOf(new Function<Map.Entry<String, LongAdder>, Long>() {
                    public Long apply(Map.Entry<String, LongAdder> entry) {
                        return entry.getValue().sum();
                    }
                }).reverse();

        // Figure out what order of the keys is when we sort by value
        List<String> sortedKeyOrder = new LinkedList<>();
        List<Map.Entry<String, LongAdder>> resultOrdered = entryOrdering.sortedCopy(stats.entrySet());

        for (Map.Entry<String, LongAdder> entry : resultOrdered) {
            sortedKeyOrder.add(entry.getKey());
        }

        // Put the results into an immutable map ordered by value, converting LongAdder to an Integer
        ImmutableSortedMap.Builder<String, Integer> builder =
                new ImmutableSortedMap.Builder<>(Ordering.explicit(sortedKeyOrder));

        // Multiply the fine totals by however many tickets we skip per ticket + 1
        // to arrive at our best guess of the real fine totals for each street
        for (Map.Entry<String, LongAdder> entry : resultOrdered) {
            builder.put(entry.getKey(), entry.getValue().intValue() * (SKIP_NUM_TICKETS + 1));
        }
        return builder.build();
    }
}