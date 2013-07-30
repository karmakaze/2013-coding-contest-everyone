package ca.kijiji.contest;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.SortedMap;

public class ParkingTicketsStats {

    public static SortedMap<String, Integer> sortStreetsByProfitability(InputStream parkingTicketsStream) {
        ParkingTagsReader reader = new ParkingTagsReader(parkingTicketsStream, false);
        ParkingTagData data = new ParkingTagData();
        HashMap<String, Integer> unsortedProfitabilityByStreet = new HashMap<String, Integer>();
        
        try {
        	reader.start();
        } catch (IOException ioe) {
        	return null;
        }
        
        while (reader.readTag(data)) {
        	String streetName = data.streetNameFromLocation2();
        	
        	if (streetName != null) {
        		Integer totalFine = unsortedProfitabilityByStreet.get(streetName);
        		if (totalFine == null) {
        			totalFine = new Integer(data.set_fine_amount);
        		} else {
        			totalFine = totalFine + Integer.decode(data.set_fine_amount);
        		}
        		unsortedProfitabilityByStreet.put(streetName, totalFine);
        	}
        }
        
        SortedMapByValue<String, Integer> sortedProfitabilityByStreet = new SortedMapByValue<String, Integer>();
        sortedProfitabilityByStreet.entrySet().addAll(unsortedProfitabilityByStreet.entrySet());
        
        return sortedProfitabilityByStreet;
    } 
    
}