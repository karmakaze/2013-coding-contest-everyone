package ca.kijiji.contest;

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

	public enum ThreadingScheme {
		SingleThreaded,
		MultiThreaded
	}
	
	public enum ParsingScheme {
		Regex,
		Scanning,
		Splitting
	}
	
	final static boolean parseSignificantDataOnly = true;
	final static int dataChunkSize = 10 * 1024 * 1024;
	
	final static ThreadingScheme threadingScheme = ThreadingScheme.MultiThreaded;
	final static ParsingScheme parsingScheme = ParsingScheme.Splitting;

    public static SortedMap<String, Integer> sortStreetsByProfitability(InputStream parkingTicketsStream) {
    	BufferedReader parkingTicketsReader = null;
    	
    	try {
        	parkingTicketsReader = new BufferedReader(new InputStreamReader(parkingTicketsStream, "ascii"));
        	parkingTicketsReader.readLine();
        } catch (IOException ioe) {
        	return null;
        }
    	
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
    	
    	SortedMapByValue<String, Integer> sortedProfitabilityByStreet = new SortedMapByValue<String, Integer>();
        sortedProfitabilityByStreet.entrySet().addAll(unsortedProfitabilityByStreet.entrySet());
        
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
    	Callable<HashMap<String, Integer>> processor = new TagDataChunkProcessor(parkingTicketsReader);
    	HashMap<String, Integer> unsortedProfitabilityByStreet = null;
		try {
			unsortedProfitabilityByStreet = processor.call();
		} catch (Exception e) {
			return null;
		}
    	
        return unsortedProfitabilityByStreet;
    }
    
     /**
     * MultiThreadedWithRegex Approach
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
    	
    	try {
    		do {
    			dataChunk = new char[dataChunkSize + 256];
    			charactersRead = parkingTicketsReader.read(dataChunk, 0, dataChunkSize);
    			
    			if (charactersRead != -1) {
    				remainingLine = parkingTicketsReader.readLine();
    				
    				if (remainingLine != null && (remainingLength = remainingLine.length()) > 0) {
    					remainingLine.getChars(0, remainingLength, dataChunk, charactersRead);
    					charactersRead += remainingLength;
    				}
    				
    				dataChunkReader = new BufferedReader(new CharArrayReader(dataChunk, 0, charactersRead));
    				futures.add(consumers.submit(new TagDataChunkProcessor(dataChunkReader)));
    			}
    		} while (charactersRead != -1);
    	} catch (IOException ioe) {
    		ioe.printStackTrace();
    	}
    	
    	try {
    		consumers.shutdown();
			consumers.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
    	
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