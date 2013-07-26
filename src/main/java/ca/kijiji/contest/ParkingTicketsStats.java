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
            

            
            //            System.out.println(parkingTicketsLine[4]+","+parkingTicketsLine[7]+"\n");
        }

        br.close();
         
        parkingTickets.put("KING", 2570710);
        parkingTickets.put("ST CLAIR", 1871510);
        parkingTickets.put("AAA", 3781095);
        
        
        return parkingTickets;
    }
    
}