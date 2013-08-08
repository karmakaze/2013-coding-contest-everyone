package ca.kijiji.contest;

import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A runnable task to take strings off a queue, parse out street and fine details from those strings
 * and store that data
 * @author djmorton
 */
public class WorkerTask implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(WorkerTask.class);
    
    private static final int ADDRESS_INDEX = 0;
    private static final int FINE_INDEX = 1;
    
    private static final int FINE_FIELD_START_COMMA_STRIDE = 4;
    private static final int ADDRESS_FIELD_START_COMMA_STRIDE = 2;
    
    private final Queue<String> workQueue;
    private final AddressExtractor addressExtractor;
    private final ConcurrentHashMap<String, AtomicInteger> streetFineMap;
    private final Future<?> fileReadTaskFuture;
    
    public WorkerTask(
            final Queue<String> workQueue, 
            final AddressExtractor addressExtractor,
            final ConcurrentHashMap<String, AtomicInteger> streetFineMap,
            final Future<?> fileReadTaskFuture) {
        
        this.workQueue = workQueue;
        this.addressExtractor = addressExtractor;
        this.streetFineMap = streetFineMap;
        this.fileReadTaskFuture = fileReadTaskFuture;
    }
    
    @Override
    public void run() {
        LOG.info("Starting worker processing...");
        
        boolean didWork = false;
        
        while (true) {
            final String line = workQueue.poll();
            
            //Exit our processing loop once the file reader task has completed and
            //the work queue is empty
            if (fileReadTaskFuture.isDone() && line == null) {
                break;
            }
            
            if (StringUtils.isNotEmpty(line)) {
                didWork = true;
                final String[] fields = extractRelevantFields(line);
                
                if (StringUtils.isEmpty(fields[ADDRESS_INDEX])) {
                    continue;
                }

                storeFineData(fields);
            } else {
                didWork = false;
            }
            
            if (!didWork) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    /*
     * Place the parsed street and fine data into a map, incrementing the fine value if the street
     * is already present.
     */
    private void storeFineData(final String[] fields) {
        final String address = fields[ADDRESS_INDEX];
        final String fine = fields[FINE_INDEX];
        
        final int parsedFine = Integer.parseInt(fine);
        
        final AtomicInteger streetFine = streetFineMap.get(address);
        
        if (streetFine == null) {
            
            final AtomicInteger previousData = streetFineMap.putIfAbsent(address, new AtomicInteger(parsedFine));
            if (previousData != null) {
                previousData.addAndGet(parsedFine);
            }
        } else {
            streetFine.addAndGet(parsedFine);
        }        
    }
    
    /*
     * Reads through the line parameter determining the positions of the relevant fields
     * by the number of commas found, and extracting the appropriate substrings for the
     * fine amount of address.  Slightly faster than using String.split and creates
     * fewer objects on the heap.
     */
    private String[] extractRelevantFields(final String line) {
        
        int lastIndex = 0;
        int commaCount = 0;
        
        while (commaCount < FINE_FIELD_START_COMMA_STRIDE) {
            lastIndex = line.indexOf(',', lastIndex + 1);
            commaCount++;
        }
        
        final int startFineIndex = lastIndex;
        lastIndex = line.indexOf(',', lastIndex + 1);
        
        final String fine = line.substring(startFineIndex + 1, lastIndex);
        
        commaCount = 0;
        while (commaCount < ADDRESS_FIELD_START_COMMA_STRIDE) {
            lastIndex = line.indexOf(',', lastIndex + 1);
            commaCount++;
        }   
        
        final int startAddressIndex = lastIndex;
        lastIndex = line.indexOf(',', lastIndex + 1);
        
        final String fullAddress = line.substring(startAddressIndex + 1, lastIndex);
        final String address = addressExtractor.extractStreetFromAddress(fullAddress);
        return new String[] { address, fine };
    }

}
