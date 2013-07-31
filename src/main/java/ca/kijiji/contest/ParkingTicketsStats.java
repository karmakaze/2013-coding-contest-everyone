package ca.kijiji.contest;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Grabs street fine data given the input to return a list of streets ordered by revenue
 * 
 * @author Eamonn Watson
 */

public class ParkingTicketsStats {

    /**
     * Reads the file, extracts the street name and fine amount and create a map of those streets.
     * 
     * @param parkingTicketsStream
     * @return map of street names to revenue
     */
    public static SortedMap<String, Integer> sortStreetsByProfitability(InputStream parkingTicketsStream) 
            throws IOException {

        //Create bufferedreader to bring in the inputstream
        BufferedReader br = new BufferedReader(new InputStreamReader(parkingTicketsStream));
        SortedMap<String, Integer> parkingTickets = new TreeMap();
        String inputLine;
        
        // skips first line as it contains headers
        inputLine = br.readLine();
        
        while ((inputLine = br.readLine()) != null)   {
            //Split the line into segments based on the comma delimitation.
            String[] parkingTicketsLine = inputLine.split(",");
            String streetName = StreetClass.getStreetName(parkingTicketsLine[7]);
            
            //Skip if there is no streetname
            if (streetName.equals("")) {
                continue;
            }
            
            // grabs the current parking ticket value to detmine if we have dealt
            // with this street already
            Integer currValue = parkingTickets.get(streetName);
            if (currValue == null){
                // no current element so add it
                parkingTickets.put(streetName, new Integer(parkingTicketsLine[4]));
            } else {
                // current element exists so add to it
                parkingTickets.put(streetName, currValue + new Integer(parkingTicketsLine[4]));
            }
        }

        br.close();
                
        // now that i have the sorted by key map I now need to make the sorted by value map.
        SortedMap valueMap = new TreeMap(new ValueComparator(parkingTickets));
        valueMap.putAll(parkingTickets);
        
        return valueMap;
    }
    
}