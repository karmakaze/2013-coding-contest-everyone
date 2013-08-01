package ca.kijiji.contest;

import au.com.bytecode.opencsv.CSVParser;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Ordering;
import com.google.common.io.LineReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.transform.Source;
import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class ParkingTicketsStats {

    // tools for the job threads
    public static ConcurrentHashMap<String, Integer> streets = new ConcurrentHashMap<String, Integer>();
    private static final int threadPoolSize = 4; // speed seems to max out at 4 threads for my machine

    public static SortedMap<String, Integer> sortStreetsByProfitability(InputStream parkingTicketsStream) throws Exception {
        LineReader lr = new LineReader(new BufferedReader(new InputStreamReader(parkingTicketsStream)));

        String line = lr.readLine(); // ignore first line is has the csv header
        line = lr.readLine();
        ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize);
        // walk over the csv and fire off job threads for each line
        while (line != null) {
            Runnable job = new JobThread(line);
            executor.execute(job);

            line = lr.readLine();
        }

        // thread cleanup
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);
        while (!executor.isTerminated()) {
        }

        // the cool quest for "a sortedMap sorted by value not keys" google guava to the rescue
        // thanks to http://stackoverflow.com/questions/109383/how-to-sort-a-mapkey-value-on-the-values-in-java for a nice solution
        // with reverse sort twist
        Comparator valueReverseComparator = Ordering.natural().reverse().onResultOf(Functions.forMap(streets)).compound(Ordering.natural());
        SortedMap sortedStreets = ImmutableSortedMap.copyOf(streets, valueReverseComparator);

        //prettyPrintMap(sortedStreets);

        return sortedStreets;
    }

    /** Scrubs a street name using some regex.
     * Doesn't do a perfect job, but there are always outliers in a large dataset. Shouldn't hurt too much.
     *
     * returns a cleaner version of street name
     * */
    public static String cleanupStreetName(String street) {
        // cleanup street name
        street = specialChars.matcher(street).replaceAll("").trim();
        street = leadingDigits.matcher(street).replaceAll("").trim();
        street = trailingDigits.matcher(street).replaceAll("").trim();
        if (street.contains("UNIT")) {
            street = unit1.matcher(street).replaceAll("").trim();
            street = unit2.matcher(street).replaceAll("").trim();
            street = unit3.matcher(street).replaceAll("").trim();
        }
        street = compass.matcher(street).replaceAll("").trim();
        street = streetType.matcher(street).replaceAll("").trim();

        return street;
    }

    /** for debug */
    public static void prettyPrintMap(SortedMap<String, Integer> streets) throws InterruptedException {
        for (Map.Entry<String, Integer> item : streets.entrySet()) {
            LOG.info(String.format("{ street: %s, profit: %s }", item.getKey(), item.getValue()));
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(ParkingTicketsStats.class);

    /* some pre-compiled REGEX patterns for scrubbing, used by cleanupStreetName */
    /* remove possible typos and special chars for example <=>!@#$%^&*-+()'?".,[]:;`/\   */
    private static final Pattern specialChars = Pattern.compile("[\\<\\=\\>\\!\\@\\#\\$\\%\\^\\&\\*\\-\\+\\(\\)\\'\\?\\\"\\.\\,\\[\\]\\:\\;\\`\\/\\\\]");
    private static final Pattern leadingDigits = Pattern.compile("\\d+[A-Z]? ");
    private static final Pattern trailingDigits = Pattern.compile("\\d+[A-Z]?$");
    // if(street.contains("UNIT")) {
    private static final Pattern unit1 = Pattern.compile(" (NR UNIT|UNIT)");
    private static final Pattern unit2 = Pattern.compile("^UNIT ");
    private static final Pattern unit3 = Pattern.compile(" NEAR$");
    // compass directions & street types
    private static final Pattern compass = Pattern.compile(" (N|NORTH|E|EAST|S|SOUTH|W|WEST)$");
    private static final Pattern streetType = Pattern.compile(" (ROAD|R|RD|CRESCENT|CRES|STREET|STEET|STT|STR|T|ST|GATE|GT|" +
            "COURT|CRT|CR|CT|CIRCLE|CIR|HEIGHTS|HTS|AVENUE|AVE|AV|LANE|LA|LN|DRIVE|DR|PLACE|PL|PARK|PK|" +
            "SQUARE|SQARE|SQ|GROVE|GRV|BOULEVARD|BL|BLV|BLVD|TERRACE|TER|TR|T|HALL|WAY|WY|GARDENS|GDNS|PARKWAY|PKWY|PTWY)$");
}