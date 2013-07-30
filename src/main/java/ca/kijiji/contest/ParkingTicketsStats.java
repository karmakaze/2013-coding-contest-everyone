package ca.kijiji.contest;

import java.io.BufferedReader;
import java.io.CharArrayReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.SortedMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ParkingTicketsStats {
	
	public enum Approach {
		SingleThreadedWithRegex,
		MultiThreadedWithRegex,
		SingleThreadedWithComponents,
		MultiThreadedWithComponents
	}
	
	final static boolean parseSignificantDataOnly = true;
	final static int dataChunkSize = 10 * 1024 * 1024;
	final static Approach approach = Approach.SingleThreadedWithRegex;

    public static SortedMap<String, Integer> sortStreetsByProfitability(InputStream parkingTicketsStream) {
    	switch (approach) {
    		case SingleThreadedWithRegex:
    			return sortStreetByProfitabilityUsingSingleThreadWithRegex(parkingTicketsStream);
    		
    		case MultiThreadedWithRegex:
    			return sortStreetByProfitabilityUsingMultipleThreadsWithRegex(parkingTicketsStream);
    			
    		default:
    			throw new UnsupportedOperationException();
    	}
    }
    
    /**
     * SingleThreadedWithRegex Approach
     * 
     * This approach uses a BufferedReader to read in from parkingTicketsStream line by line, parsing out
     * the street name and fine amount, and updating unsortedProfitabilityByStreet along the way.
     * 
     * @param parkingTicketsStream
     * @return
     */
    static SortedMap<String, Integer> sortStreetByProfitabilityUsingSingleThreadWithRegex(InputStream parkingTicketsStream) {
    	BufferedReader parkingTicketsReader = null;
    	
    	try {
        	parkingTicketsReader = new BufferedReader(new InputStreamReader(parkingTicketsStream, "ascii"));
        	parkingTicketsReader.readLine();
        } catch (IOException ioe) {
        	return null;
        }
        
    	Callable<HashMap<String, Integer>> processor = new TagDataChunkProcessor(parkingTicketsReader);
    	HashMap<String, Integer> unsortedProfitabilityByStreet = null;
		try {
			unsortedProfitabilityByStreet = processor.call();
		} catch (Exception e) {
			return null;
		}
    	
    	SortedMapByValue<String, Integer> sortedProfitabilityByStreet = new SortedMapByValue<String, Integer>();
        sortedProfitabilityByStreet.entrySet().addAll(unsortedProfitabilityByStreet.entrySet());
        
        return sortedProfitabilityByStreet;
    }
    
     /**
     * MultiThreadedWithRegex Approach
     * 
     * This approach reads in chars in approximately 10 MB chunks (always ending at the end of a line)
     * on the main thread, and uses an ExecutorService to parse them.
     * 
     * Based on http://stackoverflow.com/questions/2332537/producer-consumer-threads-using-a-queue
     * 
     * @param parkingTicketsStream
     * @return
     */
    static SortedMap<String, Integer> sortStreetByProfitabilityUsingMultipleThreadsWithRegex(InputStream parkingTicketsStream) {
    	BufferedReader parkingTicketsReader = null;
    	
    	try {
        	parkingTicketsReader = new BufferedReader(new InputStreamReader(parkingTicketsStream, "ascii"));
        	parkingTicketsReader.readLine();
        } catch (IOException ioe) {
        	return null;
        }
    	
    	// Prepare an ExecutorService to process incoming data chunks
    	int cores = Runtime.getRuntime().availableProcessors();
    	ExecutorService consumers = Executors.newFixedThreadPool(cores);
    	
    	
    	char[] dataChunk = null;
    	String remainingLine = null;
    	int charactersRead = -1;
    	int remainingLength = 0;
    	
    	try {
    		do {
    			dataChunk = new char[dataChunkSize + 256];
    			charactersRead = parkingTicketsReader.read(dataChunk, 0, dataChunkSize);
    			
    			if (charactersRead != -1) {
    				remainingLine = parkingTicketsReader.readLine();
    				
    				if ((remainingLength = remainingLine.length()) > 0) {
    					remainingLine.getChars(0, remainingLength, dataChunk, charactersRead);
    					charactersRead += remainingLength;
    				}
    				
    				BufferedReader dataChunkReader = new BufferedReader(new CharArrayReader(dataChunk, 0, charactersRead));
    				
    				consumers.submit(new TagDataChunkProcessor(dataChunkReader));
    			}
    		} while (charactersRead != -1);
    	} catch (IOException ioe) {
    		return null;
    	}
    	
    	return null;
    }
    
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
                		if ((streetName = data.streetNameFromLocation2UsingRegex()) != null) {
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