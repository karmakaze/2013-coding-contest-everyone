package ca.kijiji.contest;

import java.io.InputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListMap;

public class ParkingTicketsStats {

    private static final int THREAD_COUNT = 5;
    private static final int FILE_INPUT_BUFFER_SIZE = 2621440;
    private static final int INPUT_QUEUE_LENGTH = 5000;

    private static final int FINE_AMOUNT_COLUMN = 4;
    private static final int STREET_ADDRESS_COLUMN = 7;

    // Save a string allocation by using a final blank string for replacement when cleaning street names
    private static final String BLANK = "";

    private static ArrayBlockingQueue<String> lineQueue = new ArrayBlockingQueue<String>(INPUT_QUEUE_LENGTH);
    private static SortedMap<String, Integer> output = new ConcurrentSkipListMap<String, Integer>();

    // Build the cleaning regex only once
    private static final String cleaningRegex = buildRegex();
    private static final String buildRegex() {
        // Programmatically building a regex through string concatenation... and now I have TWO problems!
        // But seriously this regex cleans up and normalizes the input street name data in a reasonably effective way
        final String[] suffixes = {"ST", "STREET", "AVE", "AV", "COURT", "CT", "CRT", "DRIVE", "DR",
                "BOULEVARD", "BLVD", "ROAD", "RD", "CRESCENT", "CRES", "TERRACE", "TER", "EAST", "WEST", "ST E",
                "ST W", "ST N", "ST S", "AVE W", "AVE E", "AVE S", "AVE N"};
        StringBuffer suffixRegex = new StringBuffer("|\\s+(");
        for (String s: suffixes) {
            suffixRegex.append(s + "|");
        }
        suffixRegex.deleteCharAt(suffixRegex.length() - 1);
        suffixRegex.append(")*$");
        String baseRegex = "[^A-Z\\s]+";
        String result = baseRegex + suffixRegex;
        return result;
    }

    public final static String cleanStreet(String value) {
        return value.replaceAll(cleaningRegex, BLANK).trim();
    }

    // Thanks to http://hmkcode.com/sorting-java-map-by-key-value/
    // Sneakily reorders a map by value instead of key
    public static SortedMap<String, Integer> sortByValue(Map<String, Integer> unsortedMap){

        class ValueComparator<String> implements Comparator<String> {
            Map<String, Integer> map;

            public ValueComparator(Map<String, Integer> map){
                this.map = map;
            }

            public int compare(String keyA, String keyB) {
                return map.get(keyB).compareTo(map.get(keyA));
            }
        }
        SortedMap<String, Integer> sortedMap = new TreeMap<String, Integer>(new ValueComparator<String>(unsortedMap));
        sortedMap.putAll(unsortedMap);
        return sortedMap;
    }

    public static SortedMap<String, Integer> sortStreetsByProfitability(InputStream parkingTicketsStream) {

        class FileScanner extends Thread {

            // Prevent as many allocations in our execution loop as we can
            private String line, street;
            private Integer currentSum;
            private int cost;

            private final BlockingQueue<String> inputQueue;
            private final SortedMap<String, Integer> stats;

            // A class to read input lines from a synchronized queue and parse out their values, then update the map
            public FileScanner(SortedMap<String, Integer> repository, BlockingQueue<String> input) {
                inputQueue = input;
                stats = repository;
            }

            public void run() {
                while(true) {
                    // Continue grabbing lines off the queue until interrupted by the parent (indicating an empty queue)
                    try {
                        line = inputQueue.take();

                        // Now we've got the line, split it and grab the address and costs
                        String[] values = line.split(",");

                        // Get a normalized version of the street name, and skip it if it ends up being blank
                        street = cleanStreet(values[STREET_ADDRESS_COLUMN]);
                        if (street == "") continue;

                        // Get the ticket cost as an integer
                        cost = Integer.parseInt(values[FINE_AMOUNT_COLUMN]);

                        // Update the stats in the map, Technically there's a race condition here but hey, we don't have
                        // to be perfect, right? We'd probably only ever lose a handful of ticket values, if any.
                        currentSum = stats.get(street);
                        if (currentSum == null) currentSum = 0;
                        stats.put(street, currentSum+cost);
                    }
                    catch(InterruptedException e) {
                        // An interruption means the Queue AND the source file are empty, (Or that the program was
                        // halted with a ctrl-c or something) so this threads work is done.
                        break;
                    }
                }
            }
        }

        // Initialize and start our filescanners, they'll block on reading the queue until we feed in data
        ArrayList<FileScanner> threadList = new ArrayList<FileScanner>(THREAD_COUNT);
        for (int i = 0; i < THREAD_COUNT; i++) {
            FileScanner fs = new FileScanner(output, lineQueue);
            fs.start();
            threadList.add(fs);
        }

        try {
            BufferedReader inputFile = new BufferedReader(new InputStreamReader(parkingTicketsStream),
                    FILE_INPUT_BUFFER_SIZE);

            // Skip the header line for the input stream
            inputFile.readLine();

            // Read lines into our input queue for the threads to consume
            String line = inputFile.readLine();
            while(line != null) {
                lineQueue.put(line);
                line = inputFile.readLine();
            }

            // Once we've finished reading the lines in, wait until the threads have consumed all available lines
            while (lineQueue.peek() != null) {
                Thread.sleep(50);
            }

            // If all the lines are gone, interrupt the readers to clean them up
            for (FileScanner fs: threadList) {
                fs.interrupt();
                fs.join();
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            return null;
        }

        // Sort our map by value (which is a bit naughty :3) then return it
        return ParkingTicketsStats.sortByValue(output);
    }
}