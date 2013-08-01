package ca.kijiji.contest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
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
		SingleThreaded,		// Read and process tickets on the main thread
		MultiThreaded,		// Read on the current thread and process on multiple other threads
		Testing				// Testing
	}
	
	enum ParsingScheme {
		Regex,				// Use regular expressions to extract street names
		Scanning,			// Use a string scanner to extract street names
		Splitting			// Split strings and trim components to extract street names
	}

	final static Logger LOG = LoggerFactory.getLogger(ParkingTicketsStats.class);
	
	final static boolean parseSignificantDataOnly = true;
	final static int dataChunkSize = 10 * 1024 * 1024;
	
	final static ThreadingScheme threadingScheme = ThreadingScheme.MultiThreaded;
	final static ParsingScheme parsingScheme = ParsingScheme.Splitting;

    public static SortedMap<String, Integer> sortStreetsByProfitability(InputStream parkingTicketsStream) {
    	// Using the specified threading scheme, get an unsorted map of streets and their profitability
    	LOG.info("Sorting parking tickets using the {} and {} schemes.", threadingScheme, parsingScheme);
    	
    	Map<String, Integer> unsortedProfitabilityByStreet = null;
    	
    	switch (threadingScheme) {
    		case SingleThreaded:
    			unsortedProfitabilityByStreet = streetsByProfitabilityUsingSingleThread(parkingTicketsStream);
    			break;
    		
    		case MultiThreaded:
    			unsortedProfitabilityByStreet = streetsByProfitabilityUsingMultipleThreads(parkingTicketsStream);
    			break;
    			
    		case Testing:
    			unsortedProfitabilityByStreet = streetsByProfitabilityTesting(parkingTicketsStream);
    			
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
     * This approach simply creates a TagDataChunkProcessor for the passed in parkingTicketsReader
     * and runs it on the current thread. The chunk in this case is the entire source file.
     * 
     * @param parkingTicketsStream
     * @return An unsorted Map of streets and revenues.
     */
    static Map<String, Integer> streetsByProfitabilityUsingSingleThread(InputStream parkingTicketsStream) {
    	BufferedReader parkingTicketsReader = null;
    	
    	// Wrap parkingTicketsStream in a BufferedReader and read (throw away) the header line
    	try {
        	parkingTicketsReader = new BufferedReader(new InputStreamReader(parkingTicketsStream, "ascii"));
        	parkingTicketsReader.readLine();
        } catch (IOException ioe) {
        	LOG.error("An error occurred creating parkingTicketsReader and reading the header line.");
        	
        	return null;
        }
    	
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
     * This approach reads in chars from parkingTicketsReader in approximately 10 MB chunk on the
     * current thread, then create a TagDataChunkProcessor with each data chunk and uses an
     * ExecutorService to schedule and run it on multiple threads. The ExecutorService is set to use
	 * a thread pool sized to match the number of avaiable processors (presumeably real and hyperthreaded).
	 * 
	 * The TagDataChunkProcessors all provide an intermediate Map of streets and revenues, which are
	 * reduced to a single Map that is returned to the caller.
     * 
     * Based on http://stackoverflow.com/questions/2332537/producer-consumer-threads-using-a-queue
     * 
     * @param parkingTicketsStream
     * @return An unsorted Map of streets and revenues.
     */
    //static Map<String, Integer> streetsByProfitabilityUsingMultipleThreads(BufferedReader parkingTicketsReader) {
    static Map<String, Integer> streetsByProfitabilityUsingMultipleThreads(InputStream parkingTicketsStream) {
    	int cores = Runtime.getRuntime().availableProcessors();
    	ExecutorService consumers = Executors.newFixedThreadPool(cores);
    	ArrayList<Future<HashMap<String, Integer>>> futures = new ArrayList<Future<HashMap<String, Integer>>>(30);
    	
    	byte[] dataChunk = null;
    	int bytesRead = -1;
    	int extraByte = -1;
    	BufferedReader dataChunkReader = null;
    	
    	// Throw away the first line of data in the input stream (the header)
    	try {
    		do {
        		extraByte = parkingTicketsStream.read();
        		
        		if (extraByte == 13) {
        			extraByte = parkingTicketsStream.read();
        		}
        		
        		if (extraByte == 10) {
    				break;
    			}
        	} while (true);
    	} catch (IOException ioe) {
    		return null;
    	}
    	
    	// Read in parkingTickets data in dataChunkSize chunks, then create TagDataChunkProcessor
    	// instances for each chunk and dispatched them to the consumers ExecutorService.
    	long procStartTime = System.currentTimeMillis();
    	
    	try {
    		do {
    			// dataChunk is initialized with space for an extra 256 characters
    			// to cover reading to the end of a line after dataChunkSize characters
    			// are read. That way each chunk ends with a complete line.
    			// Lines appear to be less than 100 characters long, normally.
    			dataChunk = new byte[dataChunkSize + 160];
    			bytesRead = parkingTicketsStream.read(dataChunk, 0, dataChunkSize);
    			
    			if (bytesRead != -1) {
    				// keep reading bytes until we find CRLF
    				do {
    					extraByte = parkingTicketsStream.read();
    					
    					if (extraByte == -1) {
    						break;
    					}
    					
    					if (extraByte == 13) {
    						extraByte = parkingTicketsStream.read();
    					}
    					
    					if (extraByte == 10) {
							break;
						}
    					
    					dataChunk[bytesRead++] = (byte)extraByte;
    				} while (true);
    				
    				// Create a TagDataChunkProcessor and submit it to the ExecutorService for running
    				dataChunkReader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(dataChunk, 0, bytesRead)));
    				futures.add(consumers.submit(new TagDataChunkProcessor(dataChunkReader)));
    			}
    		} while (bytesRead != -1);
    	} catch (IOException ioe) {
    		ioe.printStackTrace();
    	}
    	
    	long chunkDuration = System.currentTimeMillis() - procStartTime;
        LOG.info("Duration of data chunking = {} ms", chunkDuration);
    	
        // Stop accepting new dispatches and wait for all the chunk processors to finish
    	try {
    		consumers.shutdown();
			consumers.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
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
    
    static Map<String, Integer> streetsByProfitabilityTesting(InputStream parkingTicketsStream) {
    	BufferedReader parkingTicketsReader = null;
    	
    	// Wrap parkingTicketsStream in a BufferedReader and read (throw away) the header line
    	try {
        	parkingTicketsReader = new BufferedReader(new InputStreamReader(parkingTicketsStream, "ascii"));
        	parkingTicketsReader.readLine();
        } catch (IOException ioe) {
        	LOG.error("An error occurred creating parkingTicketsReader and reading the header line.");
        	
        	return null;
        }
    	
    	long procStartTime = System.currentTimeMillis();
    	
    	try {
			while (parkingTicketsReader.readLine() != null) {
				
			}
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		
		long procDuration = System.currentTimeMillis() - procStartTime;
        LOG.info("Duration of data reading and processing = {} ms", procDuration);
    	
        return null;
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
        	
            // For each line from reader, use the ParkingTagData instance to parse the line
            // and get the street name and fine amount, then accumulate the fines in
            // unsortedProfitabilityByStreet.
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