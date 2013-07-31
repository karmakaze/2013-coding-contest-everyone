package ca.kijiji.contest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.CharArrayReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class ParkingTicketsStats {

	enum ThreadingScheme {
		SingleThreaded,
		MultiThreaded
	}
	
	enum ParsingScheme {
		Regex,
		Scanning,
		Splitting
	}

	final static Logger LOG = LoggerFactory.getLogger(ParkingTicketsStats.class);
	
	final static boolean parseSignificantDataOnly = true;
	final static int dataChunkSize = 10 * 1024 * 1024;
	
	final static ThreadingScheme threadingScheme = ThreadingScheme.SingleThreaded;
	final static ParsingScheme parsingScheme = ParsingScheme.Splitting;

    public static SortedMap<String, Integer> sortStreetsByProfitability(InputStream parkingTicketsStream) {
    	BufferedReader parkingTicketsReader = null;
    	
    	// Wrap parkingTicketsStream in a BufferedReader and read (throw away) the header line
    	try {
        	parkingTicketsReader = new BufferedReader(new InputStreamReader(parkingTicketsStream, "ascii"));
        	parkingTicketsReader.readLine();
        } catch (IOException ioe) {
        	LOG.error("An error occurred creating parkingTicketsReader and reading the header line.");
        	
        	return null;
        }
    	
    	// Using the specified threading scheme, get an unsorted map of streets and their profitability
    	LOG.info("Sorting parking tickets using the {} and {} schemes.", threadingScheme, parsingScheme);
    	
    	Map<String, Integer> unsortedProfitabilityByStreet = null;
    	
    	switch (threadingScheme) {
    		case SingleThreaded:
    			unsortedProfitabilityByStreet = streetsByProfitabilityUsingSingleThread(parkingTicketsReader);
    			break;
    		
    		case MultiThreaded:
    			unsortedProfitabilityByStreet = streetsByProfitabilityUsingMultipleThreads(parkingTicketsReader);
    			break;
    			
    		default:
    			throw new UnsupportedOperationException();
    	}
    	
    	// Sort the street and profitability data and return
    	long sortStartTime = System.currentTimeMillis();
    	
    	SortedMapByValue<String, Integer> sortedProfitabilityByStreet = new SortedMapByValue<String, Integer>();
        sortedProfitabilityByStreet.entrySet().addAll(unsortedProfitabilityByStreet.entrySet());
        
        long sortDuration = System.currentTimeMillis() - sortStartTime;
        LOG.info("Duration of sorting = {} ms", sortDuration);
        
    	return sortedProfitabilityByStreet;
    }
    
    /**
     * SingleThreaded Approach
     * 
     * This approach uses a BufferedReader to read in from parkingTicketsStream line by line, parsing out
     * the street name and fine amount, and updating unsortedProfitabilityByStreet along the way.
     * 
     * @param parkingTicketsStream
     * @return
     */
    static Map<String, Integer> streetsByProfitabilityUsingSingleThread(BufferedReader parkingTicketsReader) {
    	long procStartTime = System.currentTimeMillis();
    	
    	Callable<HashMap<String, Integer>> processor = new TagDataChunkProcessor(parkingTicketsReader);
    	HashMap<String, Integer> unsortedProfitabilityByStreet = null;
		try {
			unsortedProfitabilityByStreet = processor.call();
		} catch (Exception e) {
			return null;
		}
		
		long procDuration = System.currentTimeMillis() - procStartTime;
        LOG.info("Duration of data reading and processing = {} ms", procDuration);
    	
        return unsortedProfitabilityByStreet;
    }
    
     /**
     * MultiThreaded Approach
     * 
     * This approach reads in chars in approximately 10 MB chunks (always ending at the end of a line)
     * on the main thread, and uses an ExecutorService to manage TagDataChunkProcessor instances.
     * 
     * Based on http://stackoverflow.com/questions/2332537/producer-consumer-threads-using-a-queue
     * 
     * @param parkingTicketsStream
     * @return
     */
    static Map<String, Integer> streetsByProfitabilityUsingMultipleThreads(BufferedReader parkingTicketsReader) {
    	// Prepare an ExecutorService to process incoming data chunks
    	int cores = Runtime.getRuntime().availableProcessors();
    	ExecutorService consumers = Executors.newFixedThreadPool(cores);
    	ArrayList<Future<HashMap<String, Integer>>> futures = new ArrayList<Future<HashMap<String, Integer>>>();
    	
    	char[] dataChunk = null;
    	BufferedReader dataChunkReader = null;
    	String remainingLine = null;
    	int charactersRead = -1;
    	int remainingLength = 0;
    	
    	// Read in parkingTickets data in dataChunkSize chunks, then create TagDataChunkProcessor
    	// instances for each chunk and dispatched them to the consumers ExecutorService.
    	long procStartTime = System.currentTimeMillis();
    	
    	try {
    		do {
    			// dataChunk is initialized with space for an extra 256 characters
    			// to cover reading to the end of a line after dataChunkSize characters
    			// are read. That way each chunk ends with a complete line.
    			// Lines appear to be less than 100 characters long, normally.
    			dataChunk = new char[dataChunkSize + 256];
    			charactersRead = parkingTicketsReader.read(dataChunk, 0, dataChunkSize);
    			
    			if (charactersRead != -1) {
    				remainingLine = parkingTicketsReader.readLine();
    				
    				// Read to the end of a line and append it to dataChunk, if necessary
    				if (remainingLine != null && (remainingLength = remainingLine.length()) > 0) {
    					remainingLine.getChars(0, remainingLength, dataChunk, charactersRead);
    					charactersRead += remainingLength;
    				}
    				
    				// Create a TagDataChunkProcessor and submit it to the ExecutorService for running
    				dataChunkReader = new BufferedReader(new CharArrayReader(dataChunk, 0, charactersRead));
    				futures.add(consumers.submit(new TagDataChunkProcessor(dataChunkReader)));
    			}
    		} while (charactersRead != -1);
    	} catch (IOException ioe) {
    		ioe.printStackTrace();
    	}
    	
    	long chunkDuration = System.currentTimeMillis() - procStartTime;
        LOG.info("Duration of data chunking = {} ms", chunkDuration);
    	
        // Stop accepting new dispatches and wait for all the chunk processors to finish
    	try {
    		consumers.shutdown();
			consumers.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
    	
    	long procDuration = System.currentTimeMillis() - procStartTime;
        LOG.info("Duration of data chunking and processing = {} ms", procDuration);
    	
    	// Reduce the intermediate results from each TagDataChunkProcessor via their futures
    	long reduceStartTime = System.currentTimeMillis();
    	
    	HashMap<String, Integer> futureProfitabilityByStreet = null;
    	HashMap<String, Integer> accumulatedProfitabilityByStreet = new HashMap<String, Integer>();
    	Integer intermediateFines, accumulatedFines = null;
    	
    	try {
    		for (Future<HashMap<String, Integer>> future : futures) {
    			futureProfitabilityByStreet = future.get();
    			for (String street : futureProfitabilityByStreet.keySet()) {
    				intermediateFines = futureProfitabilityByStreet.get(street);
    				accumulatedFines = accumulatedProfitabilityByStreet.get(street);
    				accumulatedFines = (accumulatedFines != null) ? accumulatedFines + intermediateFines : intermediateFines;
    				
    				accumulatedProfitabilityByStreet.put(street, accumulatedFines);
    			}
        	}
    	} catch (InterruptedException e) {
    		e.printStackTrace();
    	} catch (ExecutionException e) {
    		e.printStackTrace();
    	}
    	
    	long reduceDuration = System.currentTimeMillis() - reduceStartTime;
        LOG.info("Duration of reduce = {} ms", reduceDuration);
    	
    	return accumulatedProfitabilityByStreet;
    }
    
    /**
     * The <code>TagDataChunkProcessor</code> Callable accepts a BufferedReader containing
     * parking tickets data to be read line-by-line and parsed into data fields. It provides
     * an unsorted HashMap of the parsed data for the caller to sort or combine with results
     * from other <code>TagDataChunkProcessor</code> instances. 
     */
    static class TagDataChunkProcessor implements Callable<HashMap<String, Integer>> {
    	
    	BufferedReader reader = null;
    	
    	TagDataChunkProcessor(BufferedReader reader) {
    		super();
    		
    		this.reader = reader;
    	}

    	public HashMap<String, Integer> call() {
    		String line = null;
        	ParkingTagData data = new ParkingTagData();
        	String streetName = null;
        	Integer totalFine = null;
            HashMap<String, Integer> unsortedProfitabilityByStreet = new HashMap<String, Integer>();
        	
            try {
            	while ((line = reader.readLine()) != null) {
                	if (data.updateFromDataLine(line, parseSignificantDataOnly)) {
                		switch(parsingScheme) {
	                		case Regex:
	                			streetName = data.streetNameFromLocation2UsingRegex();
	                			break;
	                			
	                		case Splitting:
	                			streetName = data.streetNameFromLocation2BySplitting();
	                			break;
	                			
	                		default:
	                			throw new UnsupportedOperationException();
                		}
                		
                		if (streetName != null) {
                			totalFine = unsortedProfitabilityByStreet.get(streetName);
                			totalFine = (totalFine != null) ? totalFine + data.fineAmount() : data.fineAmount();
                			
                			unsortedProfitabilityByStreet.put(streetName,  totalFine);
                		}
                	}
                }
            } catch (IOException ioe) {
            	return null;
            }
            
            return unsortedProfitabilityByStreet;
    	}
    	
    }
    
}