package ca.kijiji.contest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParkingTicketsStats {

  private static final class ParseWorker implements Callable<Map<String, Map.Entry<String, Integer>>> {
    private final BlockingQueue<String> lines;

    private ParseWorker(BlockingQueue<String> lines) {
      this.lines = lines;
    }

    public Map<String, Map.Entry<String, Integer>> call() throws Exception {
      Map<String, Map.Entry<String, Integer>> myResults = new HashMap<String, Map.Entry<String, Integer>>();
      String line;
      while ((line = lines.poll(1, TimeUnit.SECONDS)) != null) {
        try {
          LOG.debug("Read a line: {}", line);
          Entry<String, Integer> amount = StreetAmount.from(line);
          Entry<String, Integer> existingAmount = myResults.get(amount.getKey());
          if (existingAmount != null) {
            existingAmount.setValue(existingAmount.getValue() + amount.getValue());
          }
          else {
            myResults.put(amount.getKey(), amount);
          }
        }
        catch (IllegalArgumentException e) {
          // bad address
          // FIXME better exception type
        }
      }
      return myResults;
    }
  }

  // Comments on the setup of the contest:
  // 1. SortedMap is specified to be ordered by its keys, not its values. Ordering one by value is awkward and violates API users' expectations.
  // 2. Stripping off the street type (ST, RD, AVE, etc) makes the results less valid because it combines streets that should be kept separate, for example Spadina Ave and Spadina Rd.

  private static final Logger LOG = LoggerFactory.getLogger(ParkingTicketsStats.class);

  public static SortedMap<String, Integer> sortStreetsByProfitability(InputStream parkingTicketsStream) throws IOException, InterruptedException, ExecutionException {

    BufferedReader in = new BufferedReader(new InputStreamReader(parkingTicketsStream, "ascii"));

    // this skips the header line in the data
    in.readLine();

    final LinkedBlockingQueue<String> lines = new LinkedBlockingQueue<String>(100);


    ExecutorService threadPool = Executors.newFixedThreadPool(4);
    List<Future<Map<String, Entry<String, Integer>>>> parseResults = new ArrayList<Future<Map<String,Entry<String,Integer>>>>();
    for (int i = 0; i < 4; i++) {
      parseResults.add(threadPool.submit(new ParseWorker(lines)));
    }

    long startTime = System.currentTimeMillis();
    for (String line = in.readLine(); line != null; line = in.readLine()) {
      lines.offer(line, 10, TimeUnit.SECONDS);
      LOG.debug("Put a line: {}", line);
    }
    LOG.debug("File read time: {}", System.currentTimeMillis() - startTime);

    startTime = System.currentTimeMillis();
    threadPool.shutdown();
    threadPool.awaitTermination(20, TimeUnit.SECONDS);
    LOG.debug("Worker lag time: {}", System.currentTimeMillis() - startTime);

    // now merge all the results
    startTime = System.currentTimeMillis();
    Map<String, Entry<String, Integer>> combinedResults = new HashMap<String, Map.Entry<String,Integer>>();
    for (Future<Map<String, Entry<String, Integer>>> resultFuture : parseResults) {
      Map<String, Entry<String, Integer>> partialResult = resultFuture.get();
      for (Entry<String, Integer> streetValue : partialResult.values()) {
        Entry<String, Integer> entry = combinedResults.get(streetValue.getKey());
        if (entry == null) {
          combinedResults.put(streetValue.getKey(), streetValue);
        }
        else {
          entry.setValue(entry.getValue() + streetValue.getValue());
        }
      }
    }
    LOG.debug("Result combination time: {}", System.currentTimeMillis() - startTime);

    startTime = System.currentTimeMillis();
    SortedMap<String, Integer> result = new SortedByValueMap<String, Integer>(combinedResults.values());
    LOG.debug("Combined result sort time: {}", System.currentTimeMillis() - startTime);

    if (LOG.isDebugEnabled()) {
      for (Map.Entry<String, Integer> entry : result.entrySet()) {
        LOG.debug("{}", entry);
      }
    }

    return result;
  }
}