package ca.kijiji.contest;

import java.io.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Function;
import org.slf4j.*;
import com.google.common.collect.*;

import ca.kijiji.contest.ticketworkers.*;

// The input is pretty dirty (that nasty business wasn't a joke!) so you can expect things like "YONGE STRET",
// "PENGARTH CROUT" and "BEVERLEY ST BLOCKING PRIVATE DRWY" that would normally be fixed with a manual once-over...
// but let's pretend we have good data, These errors are small enough not to cause huge problems

// Don't park anywhere near Seneca college.

public class ParkingTicketsStats {

    private static final Logger LOG = LoggerFactory.getLogger(ParkingTicketsStats.class);

    // How many tickets to skip by for each one we read, 3 is a decent balance of speed and accuracy.
    private static final int SKIP_NUM_TICKETS = 3;

    // How many pending messages can be in the queue before insertions block
    private static final int MSG_QUEUE_SIZE = 4000;


    public static SortedMap<String, Integer> sortStreetsByProfitability(InputStream parkingTicketsStream)
            throws IOException, InterruptedException {

        BufferedReader parkingCsvReader = new BufferedReader(new InputStreamReader(parkingTicketsStream));

        // Normalized name cache, makes it complete around 30% faster on my PC.
        StreetNameResolver streetNameResolver = new StreetNameResolver();
        // Map of street name -> total fines
        ConcurrentHashMap<String, AtomicInteger> stats = new ConcurrentHashMap<>();

        // Leave one core for the main thread, but always use at least one worker thread.
        int numWorkerThreads = Math.max(Runtime.getRuntime().availableProcessors() - 1, 1);

        // 1 worker thread is fastest when the main thread is wasting time skipping tickets anyways.
        if(SKIP_NUM_TICKETS > 0)
            numWorkerThreads = 1;

        // Set up communication with the threads
        LinkedBlockingQueue<String> messageQueue = new LinkedBlockingQueue<>(MSG_QUEUE_SIZE);
        CountDownLatch countDownLatch = new CountDownLatch(numWorkerThreads);

        // Dispatch using a ThreadPool and Runnable for each entry was 2 times slower than manually
        // handling task dispatch, slower than the non-threaded version! Manually manage work dispatch
        // with long-running threads.

        // Set up the worker threads
        for(int i = 0; i < numWorkerThreads; ++i) {
            new StreetFineTabulator(countDownLatch, messageQueue, stats, streetNameResolver).start();
        }

        // Throw away the line with the header
        parkingCsvReader.readLine();

        // Keep sending lines to workers til we hit EOF. It's not valid to read CSVs
        // this way according to the spec, but none of the columns contain escaped newlines.
        String parkingTicketLine;
        while((parkingTicketLine = parkingCsvReader.readLine()) != null) {
            messageQueue.put(parkingTicketLine);

            // For every ticket we read, skip SKIP_NUM_TICKETS.
            for(int i=0; i < SKIP_NUM_TICKETS; ++i) {
                parkingCsvReader.readLine();
            }
        }

        // Tell the worker threads we have nothing left
        messageQueue.put(StreetFineTabulator.END_MSG);

        // Wait for the workers to finish
        countDownLatch.await();

        long startTime = System.currentTimeMillis();
        // Return an immutable map of the stats sorted by value
        SortedStatsMap foo = new SortedStatsMap(stats, SKIP_NUM_TICKETS + 1);
        long duration = System.currentTimeMillis() - startTime;
        LOG.info("Duration of sort = {} ms", duration);

        return foo;
    }
}