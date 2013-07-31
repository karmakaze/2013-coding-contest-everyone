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

  // Comments on the setup of the contest:
  // 1. SortedMap is specified to be ordered by its keys, not its values. Ordering one by value is awkward and violates API users' expectations.
  // 2. Stripping off the street type (ST, RD, AVE, etc) makes the results less valid because it combines streets that should be kept separate, for example Spadina Ave and Spadina Rd.

  private static final Logger LOG = LoggerFactory.getLogger(ParkingTicketsStats.class);

  /**
   * A concurrent worker task that takes a pre-read line from the file, parses
   * it, and accumulates the ticket value in a private map.
   *
   * @author Jonathan Fuerth <jfuerth@redhat.com>
   */
  private static final class ParseWorker implements Callable<Map<String, StreetAmount>> {
    private final BlockingQueue<String> lines;

    private ParseWorker(BlockingQueue<String> lines) {
      this.lines = lines;
    }

    public Map<String, StreetAmount> call() throws Exception {
      Map<String, StreetAmount> myResults = new HashMap<String, StreetAmount>();
      String line;
      while ((line = lines.poll(1, TimeUnit.SECONDS)) != null) {
        LOG.trace("Read a line: {}", line);
        StreetAmount amount = StreetAmountParser.parse(line);
        StreetAmount existingAmount = myResults.get(amount.getKey());
        if (existingAmount != null) {
          existingAmount.merge(amount);
        }
        else {
          myResults.put(amount.getKey(), amount);
        }
      }
      return myResults;
    }
  }

  public static SortedMap<String, Integer> sortStreetsByProfitability(InputStream parkingTicketsStream) throws IOException, InterruptedException, ExecutionException {

    BufferedReader in = new BufferedReader(new InputStreamReader(parkingTicketsStream, "ascii"));

    // skip the header line in the data file
    in.readLine();

    final LinkedBlockingQueue<String> lines = new LinkedBlockingQueue<String>(10000);

    ExecutorService threadPool = Executors.newFixedThreadPool(4);
    List<Future<Map<String, StreetAmount>>> parseResults = new ArrayList<Future<Map<String, StreetAmount>>>();
    for (int i = 0; i < 4; i++) {
      parseResults.add(threadPool.submit(new ParseWorker(lines)));
    }

    long startTime = System.currentTimeMillis();
    for (String line = in.readLine(); line != null; line = in.readLine()) {
      lines.offer(line, 10, TimeUnit.SECONDS);
      LOG.trace("Put a line: {}", line);
    }
    LOG.info("File read time: {}", System.currentTimeMillis() - startTime);

    startTime = System.currentTimeMillis();
    threadPool.shutdown();
    threadPool.awaitTermination(20, TimeUnit.SECONDS);
    LOG.info("Worker lag time: {}", System.currentTimeMillis() - startTime);

    // now merge the individual workers' results into a single map
    startTime = System.currentTimeMillis();
    Map<String, Entry<String, Integer>> combinedResults = new HashMap<String, Map.Entry<String,Integer>>();
    for (Future<Map<String, StreetAmount>> resultFuture : parseResults) {
      Map<String, StreetAmount> partialResult = resultFuture.get();
      for (StreetAmount streetAmount : partialResult.values()) {
        Entry<String, Integer> entry = combinedResults.get(streetAmount.getKey());
        if (entry == null) {
          combinedResults.put(streetAmount.getKey(), streetAmount);
        }
        else {
          entry.setValue(entry.getValue() + streetAmount.getValue());
        }
      }
    }
    LOG.info("Result combination time: {}", System.currentTimeMillis() - startTime);
    LOG.debug("Number of streets before sorting by value: {}", combinedResults.size());

    startTime = System.currentTimeMillis();
    SortedMap<String, Integer> result = new SortedByValueMap<String, Integer>(combinedResults.values());
    LOG.info("Combined result sort time: {}", System.currentTimeMillis() - startTime);
    LOG.debug("Number of streets after sorting by value: {}", result.size());

    if (LOG.isDebugEnabled()) {
      for (Map.Entry<String, Integer> entry : result.entrySet()) {
        LOG.debug("{}", entry);
      }
    }

    return result;
  }
}