/*
 * Kijiji 2013 Coding Contest: So you think you can code, eh?
 * 
 * kijijiblog.ca/so-you-think-you-can-code-eh/
 * -------------------------------------------------------------------------
 * 
 * https://github.com/daendinam/Kijiji-Coding-Contest
 * 
 * Notes: 
 * -Streets are parsed according to the definitions given in the test file ParkingTicketsStatsTest.java.
 * -A overridden comparator was created in order to solve the problem of sorting a SortedMap by decreasing value.
 * -Testing was confirmed with average Duration of computation = 3545 ms on local machine with:
 * 		-Intel i5-2500K CPU @ 3.30GHz
 * 		-16GB RAM, 64-bit Win7 OS
 * 		-Arg lines Xms1G -Xmx1G, JDK 1.7
 * 
 */

package ca.kijiji.contest;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/*
 * Analytic for parking tickets statistics; finding profitability of streets by parking ticket fines.
 * */
public class ParkingTicketsStats {
	
	private static ConcurrentHashMap<String,Integer> resultMap = new ConcurrentHashMap<String,Integer>();
	private static final Pattern regex = 
			Pattern.compile("^(?:[a-z]+\\s)?" /*Trim some bad entries with random lowercase on the front*/
					+ "(?:[\\d\\?!%\\-]+\\s)?" /*Trim street number*/
					+ "((42ND|43RD|16TH|[\\sA-Z])+?)" /*Street name pattern, account for leading digit street names*/
					+ "(?:DONWAY|LOT|PATHWAY|PTWY|ROADWAY|MEWS|PATH|PTH|PARKWAY|PKWY|GRV|GROVE|TRL|TRAIL|"
					+ "PLACE|PL|WAY|WY|HILL|CIRCLE|CIRC|CIR|CRT|COURT|CT|QUAY|PK|GATE|GT|TERRACE|TER|BOULEVARD|"
					+ "BLV|BLVD|BL|ROAD|RD|SQUARE|SQ|GARDENS|GDNS|CR|CRESCENT|CRES|LA|LANE|LN|DRIVE|DR|AVENUE|AVE|"
					+ "AV|ST|STR|STREET)?" /*Trim suffixes*/
					+ "(?:\\s(N|S|W|E|NORTH|SOUTH|EAST|WEST)$)?$"); /*Trim directions*/
	
	/*
	 * Reads ticket records from InputStream and sends individual records to be parsed and recorded to resultMap.
	 * The resultant ticket record resultMap is used to aid an overridden comparator for the return record finalMap 
	 * that sorts itself by value instead of key in decreasing monotonic order. 
	 * 
	 * The processing of ticket records in done in parallel in batches of 100 over the systems available processors.
	 * 
	 * @param parckingTicketsStream, an InputStream for the file containing parking tickets records.
	 * @return finalMap, the SortedMap of street to total fine key-value pairs.
	 * @throws InterruptedException
	 * */
    public static SortedMap<String, Integer> sortStreetsByProfitability(InputStream parkingTicketsStream) throws InterruptedException {
    	
    	String line = "";
    	BufferedReader br = null;
    	List<String> recordBatch = new ArrayList<String>();
    	
    	try {
    		//Create buffered reader from inputstream. Used over CSVReader for performance reasons.
    		br = new BufferedReader(new InputStreamReader(parkingTicketsStream,"UTF-8"));
    		
    		//Read in column headers before beginning parse.
    		line = br.readLine();
    		
    		/*Set up thread pool for feeding tasks to process ticket results in parallel. 
    		*Performance dependent on availability of processors.
    		*/
			ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
			Runnable worker = null;
			
    		//Read csv file one record at a time and feed batches of 100 to thread pool for processing.
			while((line = br.readLine()) != null) {
    			recordBatch.add(line);
    			if(recordBatch.size() == 100){
    				worker = new BatchRunnable(new ArrayList<String>(recordBatch));
    				executor.execute(worker);
    				recordBatch.clear();
    			}

    		}

    		//Feed remaining records that might total less than batch size.
    		worker = new BatchRunnable(new ArrayList<String>(recordBatch));
			executor.execute(worker);
			
    		//Wait for threads to finish.
			executor.shutdown();
    		executor.awaitTermination(60,TimeUnit.SECONDS);  
    		
    	} catch (IOException e) {
    		e.printStackTrace();
    	} finally {
    		if (br != null) {
    			try {
    				br.close();
    			} catch (IOException e) {
    				e.printStackTrace();
    			}
    		}
    	}
    	
    	/*Override comparator for SortedMap so we can populate in order of decreasing value, 
    	 * using a copy of our records(resultMap) as reference.
    	 */
    	Comparator<String> valueComparator = new Comparator<String>(){
    		@Override public int compare(String s1, String s2){
    			if(s1.compareTo(s2) == 0) {
    				return 0; /*Strings are equal, return 0 so SortedMap overwrites value*/
    			} else {			
    				return resultMap.get(s2).compareTo(resultMap.get(s1)); 
    			}
    		}
    	};
    	
    	/*Create and populate our final mapping using new valueComparator.*/
    	SortedMap<String,Integer> finalMap = new TreeMap<String,Integer>(valueComparator);
    	
    	for(Map.Entry<String,Integer> mapentry : resultMap.entrySet()) {
    		  finalMap.put(mapentry.getKey(), mapentry.getValue());
    		}
    	
    	
        return finalMap;
    }
 
    /*
     * Runnable subclass implementation for use in processing batches of ticket records in parallel.
     * Records results into resultMap.
     * */
    public static class BatchRunnable implements Runnable {
    	
    	private List<String> batch;
    	private String[] entry;
    	private String currentStreet;
    	private int currentFine;
    	private Matcher m;
    	
    	BatchRunnable(List<String> batch){
    		this.batch = batch;
    		this.entry = new String[11];
    		this.currentFine = 0;
    		this.currentStreet =  "";
    		this.m = null;
    	}
		@Override
		public void run() {
			
			for(String record : batch){
				
				/*Split csv record according to columns seperate by comma. 
				entry[4] will contain the fine column, and 
				entry[7] will contain the location2 column.*/
				entry = record.split(",");
				
				//Parse location to get street name.
				m = regex.matcher(entry[7]);
				//Account for empty streets that didnt match pattern, ignore empty or mismatched streets.
				if (m.find() && m.group(1) != null){
					
					currentStreet = m.group(1).trim();
					currentFine = Integer.parseInt(entry[4]);
					//Update hashmap with parsed street and fine key-value pair.
		    		if ( resultMap.get(currentStreet) == null ){
		    			//We don't have this street entry yet.
		    			resultMap.put(currentStreet, currentFine);
		    		} else {
		    			//We already have this street entry, readd incrementing old value by current fine.
		    			resultMap.put(currentStreet, resultMap.get(currentStreet) + currentFine);
		    		}	
				}
				
			}
		} 
    }   
}