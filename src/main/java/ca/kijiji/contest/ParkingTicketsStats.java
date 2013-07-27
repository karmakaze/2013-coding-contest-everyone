package ca.kijiji.contest;

import java.io.IOException;
import java.io.InputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.SortedMap;
import java.util.TreeMap;

public class ParkingTicketsStats {

    public static SortedMap<String, Integer> sortStreetsByProfitability(InputStream parkingTicketsStream) throws IOException {
        
        BufferedReader br = new BufferedReader(new InputStreamReader(parkingTicketsStream));
        SortedMap<String, Integer> parkingTickets = new TreeMap();
        String inputLine;
        
        // skips first line as it contains headers
        inputLine = br.readLine();
        
        while ((inputLine = br.readLine()) != null)   {
            String[] parkingTicketsLine = inputLine.split(",");
            String streetName = StreetClass.getStreetName(parkingTicketsLine[7]);
            
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
        
        //for (java.util.Map.Entry<String,Integer> entry : parkingTickets.entrySet()){
        //    System.out.println(entry.getKey());
        //}
                
        return parkingTickets;
    }
    
}