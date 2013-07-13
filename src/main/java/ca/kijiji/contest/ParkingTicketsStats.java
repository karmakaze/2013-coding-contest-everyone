package ca.kijiji.contest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParkingTicketsStats {

  // Comments on the setup of the contest:
  // 1. SortedMap is specified to be ordered by its keys, not its values. Ordering one by value is awkward and violates API users' expectations.
  // 2. Stripping off the street type (ST, RD, AVE, etc) makes the results less valid because it combines streets that should be kept separate, for example Spadina Ave and Spadina Rd.

  private static final Logger LOG = LoggerFactory.getLogger(ParkingTicketsStats.class);

  public static SortedMap<String, Integer> sortStreetsByProfitability(InputStream parkingTicketsStream) throws IOException, InterruptedException, ExecutionException {
    BufferedReader in = new BufferedReader(new InputStreamReader(parkingTicketsStream, "ascii"));

    // this skips the header line in the data
    in.readLine();

    final ConcurrentLinkedQueue<String> lines = new ConcurrentLinkedQueue<String>();

    Callable<Map<String, Map.Entry<String, Integer>>> parseWorker = new Callable<Map<String,Map.Entry<String,Integer>>>() {

      public Map<String, Map.Entry<String, Integer>> call() throws Exception {
        Map<String, Map.Entry<String, Integer>> myResults = new HashMap<String, Map.Entry<String, Integer>>();
        String line;
        while ((line = lines.poll()) != null) {
          try {
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
    };

    ExecutorService threadPool = Executors.newFixedThreadPool(4);
    Future<Map<String, Entry<String, Integer>>> parseResults = threadPool.submit(parseWorker);

    for (String line = in.readLine(); line != null; line = in.readLine()) {
      lines.add(line);
    }

    SortedMap<String, Integer> result = new SortedByValueMap<String, Integer>(parseResults.get().values());

    if (LOG.isDebugEnabled()) {
      for (Map.Entry<String, Integer> entry : result.entrySet()) {
        LOG.debug("{}", entry);
      }
    }

    return result;
  }
}