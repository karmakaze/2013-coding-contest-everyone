package ca.kijiji.contest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * @author Sergey Bushkov
 */
public class ParkingTicketsStats {
    private static final Logger LOG = LoggerFactory.getLogger(ParkingTicketsStats.class);
    private static final int NUM_THREADS = 4;
    private static final int MAX_PACKET_SIZE = 1000;
    private static final String[] END_MARKER = new String[0];

    /*
     * The stream is processed using one reader thread and several processor threads.
     *
     * The reader thread reads strings from the input stream, then puts packets of data to the working queue. Waits if
     * there is no room in the queue. When the end of the stream is reached, end markers are added to the queue (one
     * marker for each working thread).
     *
     * Processor threads take data packets from the queue; waiting if no data is available. The data goes to the
     * actual parser associated with the thread. The processor thread stops when it reads an end marker from the queue.
     *
     * Each processor creates its own street->profit map. In the end, the data is merged (similar to MapReduce),
     * and the SortedMap view is returned.
     */
    public static SortedMap<String, Integer> sortStreetsByProfitability(InputStream parkingTicketsStream) {
        BlockingQueue<String[]> queue = new ArrayBlockingQueue<>(NUM_THREADS * 2);
        Thread readerThread = new Thread(new ReaderRunnable(parkingTicketsStream, queue));
        readerThread.start();
        TicketsProcessor[] processors = new TicketsProcessor[NUM_THREADS];
        Thread[] processorThreads = new Thread[NUM_THREADS];
        for (int i = 0; i < NUM_THREADS; i++) {
            processors[i] = new TicketsProcessor();
            processorThreads[i] = new Thread(new ProcessorRunnable(processors[i], queue));
            processorThreads[i].start();
        }
        try {
            readerThread.join();
            for (int i = 0; i < NUM_THREADS; i++) {
                processorThreads[i].join();
            }
        } catch (InterruptedException e) {
            LOG.error(e.getMessage(), e);
        }
        for (int i = 1; i < NUM_THREADS; i++) {
            processors[0].mergeResult(processors[i]);
        }
        return asSortedMap(processors[0].getResult());
    }

    /*
     * Returns immutable SortedMap view of the profits map; sorted by value.
     */
    private static SortedMap<String, Integer> asSortedMap(final Map<String, Integer> profits) {
        final SortedMap<String, Integer> sortedMap = new TreeMap<>(new Comparator<String>() {
            @Override
            public int compare(String street1, String street2) {
                // do not fail when key is not found in the profits map, e.g. on sortedMap.get("UNKNOWN STREET")
                Integer f1 = profits.get(street1);
                int fine1 = f1 != null ? f1 : 0;
                Integer f2 = profits.get(street2);
                int fine2 = f2 != null ? f2 : 0;
                if (fine1 != fine2) {
                    return fine2 - fine1;
                } else {
                    return street1.compareTo(street2);
                }

            }
        });
        sortedMap.putAll(profits);
        return Collections.unmodifiableSortedMap(sortedMap);
    }

    static class ReaderRunnable implements Runnable {
        private final InputStream inputStream;
        private final BlockingQueue<String[]> queue;

        ReaderRunnable(InputStream inputStream, BlockingQueue<String[]> queue) {
            this.inputStream = inputStream;
            this.queue = queue;
        }

        @Override
        public void run() {
            try {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                    reader.readLine(); // skip the first line
                    String line;
                    String[] nextPacket = new String[MAX_PACKET_SIZE];
                    int idx = 0;
                    while ((line = reader.readLine()) != null) {
                        nextPacket[idx++] = line;
                        if (idx == MAX_PACKET_SIZE) {
                            queue.put(nextPacket);
                            nextPacket = new String[MAX_PACKET_SIZE];
                            idx = 0;
                        }
                    }
                    if (idx > 0) {
                        queue.put(Arrays.copyOf(nextPacket, idx));
                    }
                }
                for (int i = 0; i < NUM_THREADS; i++) {
                    queue.put(END_MARKER);
                }
            } catch (IOException | InterruptedException e) {
                LOG.error(e.getMessage(), e);
            }
        }
    }

    static class ProcessorRunnable implements Runnable {
        private final TicketsProcessor processor;
        private final BlockingQueue<String[]> queue;

        ProcessorRunnable(TicketsProcessor processor, BlockingQueue<String[]> queue) {
            this.processor = processor;
            this.queue = queue;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    String[] lines = queue.take();
                    if (END_MARKER == lines) {
                        break;
                    }
                    for (String line : lines) {
                        processor.processTicketRecord(line);
                    }
                }
            } catch (InterruptedException e) {
                LOG.error(e.getMessage(), e);
            }
        }
    }
}
