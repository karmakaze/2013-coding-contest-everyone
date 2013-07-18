package ca.kijiji.contest;

import java.io.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.base.Function;
import org.slf4j.*;
import com.google.common.collect.*;
import com.google.common.base.Splitter;
import com.twitter.jsr166e.LongAdder;

// The input is pretty dirty (that nasty business wasn't a joke!) so you can expect things like "YONGE STRET",
// "PENGARTH CROUT" and "BEVERLEY ST BLOCKING PRIVATE DRWY" That would normally be fixed with a manual once-over...
// but let's pretend we have good data, These errors are small enough not to cause huge problems with the data.
// Take a look with cut -d, -f8 Parking_Tags_Data_2012.csv | sed 's/\s+$//g' | awk -F' ' '{print $NF}' | sort | uniq -c | sort -nr

public class ParkingTicketsStats {

    private static final Logger LOG = LoggerFactory.getLogger(ParkingTicketsStats.class);

    private static final int CSV_BUFFER_SIZE = 20480;

    // Splits the CSV line into separate fields, *this will not work for field with escaped commas
    // and isn't compliant with the CSV spec!* But, only 23 such fields exist in the input, so we can be lazy.
    private static final Splitter FIELD_SPLITTER = Splitter.on(',');

    public static final int ADDR_COLUMN = 7;
    public static final int FINE_COLUMN = 4;

    private static final int NUM_WORKER_THREADS = 3;

    // Normalized name cache, makes it complete around 30% faster on my PC.
    private static ConcurrentHashMap<String, String> sStreetNameCache = new ConcurrentHashMap<>();





    public static SortedMap<String, Integer> sortStreetsByProfitability(InputStream parkingTicketsStream)
            throws IOException, CSVException, InterruptedException {

        // Use LongAdder instead of AtomicLong,
        ConcurrentHashMap<String, LongAdder> results = new ConcurrentHashMap<>();
        BufferedReader parkingCsvReader = new BufferedReader(new InputStreamReader(parkingTicketsStream), CSV_BUFFER_SIZE);

        // Throw away the line with the header
        parkingCsvReader.readLine();

        // Set up communication with the threads
        LinkedBlockingQueue<ParkingTicketMessage> messageQueue = new LinkedBlockingQueue<>(4000);
        CountDownLatch countDownLatch = new CountDownLatch(NUM_WORKER_THREADS);

        // Set up the threads
        for(int i = 0; i < NUM_WORKER_THREADS; ++i) {
            new ParkingTicketWorker(countDownLatch, results, sStreetNameCache, messageQueue).start();
        }

        // Keep reading lines til we hit EOF (forgive my C-isms)
        String parkingTicketLine;
        while((parkingTicketLine = parkingCsvReader.readLine()) != null) {
            //System.out.println(++i);
            // Get the column containing the address of the infraction
            String[] ticketCols = Iterables.toArray(FIELD_SPLITTER.split(parkingTicketLine), String.class);

            // Is there even an address column we could read from?
            if(ticketCols.length <= ADDR_COLUMN) {
                throw new CSVException(String.format("Malformed CSV, couldn't extract an address from line: %s", parkingTicketLine));
            }

            // If we have an address, send it off to be processed
            if(StringUtils.isNotBlank(ticketCols[ADDR_COLUMN])) {
                messageQueue.put(new ParkingTicketMessage(ticketCols));
            }
        }

        // Tell the worker threads we have nothing left
        messageQueue.put(new ParkingTicketMessage(null));

        // Wait for them all to finish
        countDownLatch.await();


        // Ordering by int value
        Ordering<Map.Entry<String, LongAdder>> entryOrdering = Ordering.natural()
            .onResultOf(new Function<Map.Entry<String, LongAdder>, Long>() {
                public Long apply(Map.Entry<String, LongAdder> entry) {
                    return entry.getValue().sum();
                }
            }).reverse();

        // Figure out what order the keys should be in to give us order by value
        List<String> sortedKeyOrder = new LinkedList<>();
        List<Map.Entry<String, LongAdder>> resultOrdered = entryOrdering.sortedCopy(results.entrySet());

        for (Map.Entry<String, LongAdder> entry : resultOrdered) {
            sortedKeyOrder.add(entry.getKey());
        }

        // Desired entries in desired order.  Put them in an ImmutableMap in this order.
        ImmutableSortedMap.Builder<String, Integer> builder = new ImmutableSortedMap.Builder<>(Ordering.explicit(sortedKeyOrder));
        for (Map.Entry<String, LongAdder> entry : resultOrdered) {
            builder.put(entry.getKey(), entry.getValue().intValue());

            //System.out.println(entry.getKey() + " : " + entry.getValue().toString());
        }
        return builder.build();
    }


    public static class CSVException extends Exception {
        public CSVException(String message) {
            super(message);
        }
    }
}