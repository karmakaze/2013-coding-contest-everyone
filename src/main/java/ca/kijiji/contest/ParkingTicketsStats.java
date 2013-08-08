package ca.kijiji.contest;

import java.io.InputStream;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class to manage the determining of the most profitable streets by parking ticket fine amounts.
 * @author djmorton
 *
 */
public class ParkingTicketsStats {
    private static final Logger LOG = LoggerFactory.getLogger(ParkingTicketsStats.class);
    
    //Chosen by experimentation as most efficient for this task, based on this implementation on both an i7 and an i3
    private static final int WORKER_THREADS = Runtime.getRuntime().availableProcessors() - 1;

    /**
     * Parses an input stream containing parking ticket data and creates a SortedMap containing that data which *does not* 
     * sort on normal lexical rules for the key string, but rather on the mapped value for each key in descending order.
     * 
     * @param parkingTicketsStream An InputStream supplying the data to be parsed
     * @return A SortedMap which sorts descending based on the magnitude of the mapped value, not on the
     * lexical ordering of the key
     */
    public static SortedMap<String, Integer> sortStreetsByProfitability(final InputStream parkingTicketsStream) {        
        final Queue<String> lineQueue = new ConcurrentLinkedQueue<>();
        final Collection<Future<?>> workerFutures = new LinkedList<>();
        final ConcurrentHashMap<String, AtomicInteger> streetFineMap = new ConcurrentHashMap<>(16, 0.75F, WORKER_THREADS);
        
        final AddressExtractor addressExtractor = new AddressExtractor();
        
        final SortedMap<String, Integer> resultMap = new TreeMap<String, Integer>(new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return streetFineMap.get(o2).get() - streetFineMap.get(o1).get();
            }            
        });

        final ExecutorService executor = Executors.newFixedThreadPool(WORKER_THREADS + 1);

        try {
            //Start the producer tasks
            final Future<?> readTaskFuture = executor.submit(new FileReaderTask(lineQueue, parkingTicketsStream));
    
            //Start the consumer tasks
            for(int i = 0; i < WORKER_THREADS; i++) {
                workerFutures.add(executor.submit(new WorkerTask(lineQueue, addressExtractor, streetFineMap, readTaskFuture)));
            }
        
            //Wait until all the worker tasks have completed
            for(Future<?> workerFuture : workerFutures) {
                workerFuture.get();
            }
            
            //Copy our ConcurrentHashMap used for storing the values into a SortedMap for returning
            for(Entry<String, AtomicInteger> entry : streetFineMap.entrySet()) {
                resultMap.put(entry.getKey(), entry.getValue().get());
            }
        } catch (InterruptedException e) {
            LOG.error("InterruptedException processing the fine info", e);
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            LOG.error("An error occurred processing the fine info", e);
        } finally {            
            cleanup(executor);
        }
        
        return resultMap;
    }
    
    private static void cleanup(final ExecutorService executor) {
        executor.shutdownNow();
    }
    
    
}
