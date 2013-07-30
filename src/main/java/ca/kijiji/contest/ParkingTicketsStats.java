package ca.kijiji.contest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.SortedMap;

public class ParkingTicketsStats {
	
	public enum Approach {
		SingleThreadedWithRegex,
		MultiThreadedWithRegex,
		SingleThreadedWithComponents,
		MultiThreadedWithComponents
	}
	
	final static boolean parseSignificantDataOnly = true;
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
    
    static SortedMap<String, Integer> sortStreetByProfitabilityUsingSingleThreadWithRegex(InputStream parkingTicketsStream) {
    	BufferedReader parkingTicketsReader = null;
    	
    	try {
        	parkingTicketsReader = new BufferedReader(new InputStreamReader(parkingTicketsStream, "ascii"));
        	parkingTicketsReader.readLine();
        } catch (IOException ioe) {
        	return null;
        }
        
    	String line = null;
    	ParkingTagData data = new ParkingTagData();
    	String streetName = null;
    	Integer totalFine = null;
        HashMap<String, Integer> unsortedProfitabilityByStreet = new HashMap<String, Integer>();
    	
        try {
        	while ((line = parkingTicketsReader.readLine()) != null) {
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
        
        
        SortedMapByValue<String, Integer> sortedProfitabilityByStreet = new SortedMapByValue<String, Integer>();
        sortedProfitabilityByStreet.entrySet().addAll(unsortedProfitabilityByStreet.entrySet());
        
        return sortedProfitabilityByStreet;
    }
    
    // 2: Multiple threads and regex
    // Based on http://stackoverflow.com/questions/2332537/producer-consumer-threads-using-a-queue
    
    static SortedMap<String, Integer> sortStreetByProfitabilityUsingMultipleThreadsWithRegex(InputStream parkingTicketsStream) {
    	return null;
    }
    
}