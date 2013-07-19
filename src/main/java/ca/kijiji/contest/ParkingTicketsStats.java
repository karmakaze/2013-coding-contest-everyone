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

// The input is pretty dirty (that nasty business wasn't a joke!) so you can expect things like "YONGE STRET",
// "PENGARTH CROUT" and "BEVERLEY ST BLOCKING PRIVATE DRWY" That would normally be fixed with a manual once-over...
// but let's pretend we have good data, These errors are small enough not to cause huge problems with the data.
// Take a look with cut -d, -f8 Parking_Tags_Data_2012.csv | sed 's/\s+$//g' | awk -F' ' '{print $NF}' | sort | uniq -c | sort -nr

public class ParkingTicketsStats {

    private static final Logger LOG = LoggerFactory.getLogger(ParkingTicketsStats.class);

    private static final int CSV_BUFFER_SIZE = 20480;


    public static SortedMap<String, Integer> sortStreetsByProfitability(InputStream parkingTicketsStream)
            throws IOException, CSVException, InterruptedException {

        BufferedReader parkingCsvReader = new BufferedReader(new InputStreamReader(parkingTicketsStream), CSV_BUFFER_SIZE);

        // Normalized name cache, makes it complete around 30% faster on my PC.
        StreetNameResolver streetNameResolver = new StreetNameResolver();
        // Use LongAdder instead of AtomicLong for performance
        ConcurrentHashMap<String, LongAdder> results = new ConcurrentHashMap<>();

        // Set up communication with the threads
        int num_threads = Math.max(1, Runtime.getRuntime().availableProcessors() - 1);

        LinkedBlockingQueue<ParkingTicketMessage> messageQueue = new LinkedBlockingQueue<>(4000);
        CountDownLatch countDownLatch = new CountDownLatch(num_threads);

        // NOTE: Dispatch using a ThreadPool and Runnable for each entry was 2 times slower than manually
        // handling task dispatch, slower than the single-threaded version. Manually manage work dispatch
        // with long-running threads.

        // Set up the threads
        for(int i = 0; i < num_threads; ++i) {
            new ParkingTicketWorker(countDownLatch, results, streetNameResolver, messageQueue).start();
        }

        // Throw away the line with the header
        parkingCsvReader.readLine();

        // Keep reading lines til we hit EOF (forgive my C-isms)
        String parkingTicketLine;
        while((parkingTicketLine = parkingCsvReader.readLine()) != null) {
            messageQueue.put(new ParkingTicketMessage(parkingTicketLine));
        }

        // Tell the worker threads we have nothing left
        messageQueue.put(ParkingTicketMessage.END);

        // Wait for them all to finish
        countDownLatch.await();

        // Return an immutable map of the stats sorted by value
        return reorderStatsMap(results);
    }

    protected static SortedMap<String, Integer> reorderStatsMap(Map<String, LongAdder> stats) {

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

        for (Map.Entry<String, LongAdder> entry : resultOrdered) {
            builder.put(entry.getKey(), entry.getValue().intValue());
        }
        return builder.build();
    }

    public static class CSVException extends Exception {
        public CSVException(String message) {
            super(message);
        }
    }
}