package ca.kijiji.contest;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;
import java.util.SortedMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ParkingTicketsStats {

    private static Logger LOG = LoggerFactory.getLogger(ParkingTicketsStats.class);

    // These static fields are used to configure the parsing process in other classes.
    // They are set here in order to have them configured in a single place

    /**
     * Similarity threshold
     * 
     * @see StreetMap#similarity(String, String)
     */
    final static double THRESHOLD;
    /**
     * Which distance function should be used: local (@see {@link StreetNames#similarity(String, String)}) or dl
     * (@see {@link DamerauLevenshteinAlgorithm#execute(String, String)})
     */
    final static String DISTANCE_FUNCTION;

    /**
     * Size in MB of the dataBlock that is passed to each thread. The number of spawned thread is
     * filesize/BYTE_BUF_SIZE. Defaults to 10 which seems to be a good guess.
     */
    private final static int BYTE_BUF_SIZE;
    /**
     * File containing "clean" street names. This file has been obtained from a different source of the Toronto
     * municipality, a spatial database that contains clean street names.
     * 
     * @see StreetNames
     */
    private final static String STREETS_FILE_NAME;

    static {
	// Init config parameters from properties file
	try {
	    Properties properties = new Properties();
	    properties.load(ParkingTicketsStats.class.getResourceAsStream("ParkingTicketsStats.properties"));
	    THRESHOLD = Double.parseDouble(properties.getProperty("threshold", "0.7"));
	    LOG.info("Set THRESHOLD to: " + THRESHOLD);
	    BYTE_BUF_SIZE = Integer.parseInt(properties.getProperty("byteBlocksize", "10"));
	    LOG.info("Set BYTE_BUF_SIZE to: " + BYTE_BUF_SIZE + " MB");
	    DISTANCE_FUNCTION = properties.getProperty("distance", "local");
	    LOG.info("Set DISTANCE_FUNCTION to: " + DISTANCE_FUNCTION);
	    STREETS_FILE_NAME = properties.getProperty("streets.resource.filename", "/centerline_clean.txt");
	} catch (Exception exc) {
	    throw new ExceptionInInitializerError(exc);
	}
    }

    /**
     * The process is in three steps:
     * <ol>
     * <li>clean street names are parsed into a StreetNames object
     * <li>the datafile is processed, matching dirty streetnames to clean street names
     * <li>sorting is applied
     * </ol>
     * 
     * The returned SortedMap is read-only: all methods that could update the map throw exceptions.
     * 
     * @param parkingTicketsStream
     * @return
     */
    public static SortedMap<String, Integer> sortStreetsByProfitability(InputStream parkingTicketsStream) {

	/*
	 * Average time for reading clean street names: 35ms
	 * Average time for reading the data file, starting the threads and waiting for them to complete: 720ms
	 * 		of which, average time for reading the data file, without starting the threads: 230ms
	 * Average time for sorting the final map: 21ms
	 * 
	 * Average total time: ~ 780ms
	 * --> Average time for processing = 720 - 230 = 490 ms. I'm not sure I can do any better than this :-)
	 * 
	 * Probably the final sorting step could be improved, but it's already so fast that the improvement would be small.
	 */

	try {
	    // Step 1 - get clean street names.
	    StreetNames cleanStreetNames = getCleanStreetNames();

	    // Step 2 - parse all data from file into streets with normalized names, matched
	    // to official street names

	    PackingStreetMap packedData = getUnsortedData(parkingTicketsStream, cleanStreetNames);

	    // Step 3 - Create final sorted map. It can't be done in the previous step
	    // because profit data change. Also, it takes very little time since no new objects
	    // are created, only the sorting tree, so it doesn't make sense to further parallelize this
	    StreetMap sortedData = makeStreetMap(packedData);
	    return sortedData;

	} catch (IOException exc) {
	    // Do nothing
	    exc.printStackTrace();
	} catch (InterruptedException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
	return null;
    }

    private static PackingStreetMap getUnsortedData(InputStream parkingTicketsStream, StreetNames cleanStreetNames)
	    throws IOException, InterruptedException {
	PackingStreetMap packedData = new PackingStreetMap();
	Object lock = new Object();
	ArrayList<PTSRunner> runners = new ArrayList<>();
	// 10 MB per thread seems to be the sweet spot for this file
	int byteBufSize = BYTE_BUF_SIZE * 1024 * 1024;
	long start = System.currentTimeMillis();
	while (true) {
	    // Use a different buffer for each runner
	    // NOTE: it's higly likely that the stream will be divided at
	    // "wrong" points, i.e. not at EOL. This means that a number of lines
	    // equal to the number of threads will probably be discarded by the parser.
	    // Given time, it could be fixed by having each thred memorize the
	    // first and last incomplete lines, then adding them back to
	    // a buffer and running a final parser. But you didn't ask for perfection :-)
	    byte[] buf = new byte[byteBufSize];
	    int read = parkingTicketsStream.read(buf);
	    if (read < 1) {
		break;
	    }
	    LOG.debug("Adding runner");
	    // Giving each runner a copy of the cleanStreetNames significantly
	    // reduces synchronization overhead. Except for the packedData and the lock,
	    // each runner works on non-shared objects
	    PTSRunner runner = new PTSRunner(buf, 0, read, cleanStreetNames.copy(), packedData, lock);
	    runners.add(runner);
	    runner.start();
	}
	LOG.info(String.format("Started %d threads in %d ms", runners.size(), (System.currentTimeMillis() - start)));
	// start = System.currentTimeMillis();
	
//	for (PTSRunner runner:runners) {
//	    runner.start();
//	}
	// Wait for the runners to complete. There is nothing to be done here except waiting :-)
	while (!runners.isEmpty()) {
	    synchronized (lock) {
		Iterator<PTSRunner> it = runners.iterator();
		boolean changed = false;
		do {
		    changed = false;
		    while (it.hasNext()) {

			PTSRunner runner = it.next();
			if (runner.isPacked()) {
			    it.remove();
			    changed = true;
			}
		    }
		} while (changed);
		if (runners.isEmpty()) {
		    break;
		}
		lock.wait();
	    }
	}
	LOG.info(String.format("All threads completed in %d ms", (System.currentTimeMillis() - start)));
	return packedData;
    }

    /**
     * Creates a StreetMap from the unsorted packed data.
     * 
     * @param pts
     * @return a new StreetMap whose underlying data is the packed data
     */
    private static StreetMap makeStreetMap(PackingStreetMap packedData) {
	long start = System.currentTimeMillis();
	StreetMap sortedData = new StreetMap(packedData);
	LOG.info("Sorting time: " + (System.currentTimeMillis() - start));
	return sortedData;
    }

    private static StreetNames getCleanStreetNames() throws IOException {
	long start = System.currentTimeMillis();
	StreetNames streetNames = new StreetNames(DISTANCE_FUNCTION);
	InputStream in = streetNames.getClass().getResourceAsStream(STREETS_FILE_NAME);
	try {
	    streetNames.parse(in);
	    LOG.info("Set-up time: " + (System.currentTimeMillis() - start));
	    return streetNames;
	} finally {
	    in.close();
	}
    }
}