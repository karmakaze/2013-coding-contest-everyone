package ca.kijiji.contest;

import gnu.trove.iterator.TObjectIntIterator;
import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.procedure.TObjectIntProcedure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.*;

/**
 * Parse input stream and return result as SortedMap. Use byte array for data
 * parsing because it's faster then common String and and requires less memory.
 * <p/>
 * Note: Non ASCII characters are not supported :'(
 * <p/>
 * User: Eugene Loykov
 * Date: 11.07.13
 * Time: 21:41
 */
public final class DataParser {
    private static final Logger LOG = LoggerFactory.getLogger(DataParser.class);

    private static final int THREADS_COUNT = Runtime.getRuntime().availableProcessors();

    private final RawDataReader reader;
    private final List<RawDataProcessor> processors = new ArrayList<>();
    private final BlockingQueue<RawData> dataQueue = new ArrayBlockingQueue<>(1000);

    public DataParser(InputStream stream) {
        this.reader = new RawDataReader(stream);
    }

    public SortedMap<String, Integer> parse() throws IOException {
        LOG.debug("Starting parse");

        final RawDataParams rawDataParams = reader.readRawDataParams();
        if (rawDataParams == null) {
            LOG.error("Can't parse raw data params");
            return null;
        }

        final List<Future> futures = runConsumers(rawDataParams);
        readData();
        waitForProcessors(futures);

        return processResults();
    }

    private void waitForProcessors(List<Future> futures) {
        for (Future future : futures) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                LOG.error(e.getMessage(), e);
            }
        }
    }

    private void readData() throws IOException {
        boolean reading = true;
        while (reading) {
            final RawData rawData = new RawData();
            reading = reader.readRawData(rawData);
            processData(rawData);
        }

        processData(RawData.POISON);
    }

    private void processData(RawData rawData) {
        try {
            dataQueue.put(rawData);
        } catch (InterruptedException e) {
            LOG.error("Data reading thread interrupted", e);
        }
    }

    private List<Future> runConsumers(RawDataParams params) {
        final List<Future> futures = new ArrayList<>(THREADS_COUNT);
        final ExecutorService service = Executors.newFixedThreadPool(THREADS_COUNT);
        for (int i = 0; i < THREADS_COUNT; i++) {
            final RawDataProcessor processor = new RawDataProcessor(dataQueue, params);
            processors.add(processor);
            futures.add(service.submit(processor));
        }

        return futures;
    }

    private TreeMap<String, Integer> processResults() {
        final TObjectIntHashMap<String> results = new TObjectIntHashMap<>();
        final TObjectIntProcedure<String> mergeResultsProcedure = new TObjectIntProcedure<String>() {
            @Override
            public boolean execute(String s, int i) {
                results.adjustOrPutValue(s, i, i);
                return true;
            }
        };

        for (RawDataProcessor processor : processors) {
            processor.getResults().forEachEntry(mergeResultsProcedure);
        }

        final TreeMap<String, Integer> resultingMap = new TreeMap<>(new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return Integer.compare(results.get(o2), results.get(o1));
            }
        });
        final TObjectIntIterator<String> iterator = results.iterator();
        while (iterator.hasNext()) {
            iterator.advance();
            resultingMap.put(iterator.key(), iterator.value());
        }

        return resultingMap;
    }
}
