package ca.kijiji.contest;

import java.io.InputStream;
import java.io.IOException;
import java.util.SortedMap;

public class ParkingTicketsStats {

    public static SortedMap<String, Integer> sortStreetsByProfitability(InputStream parkingTicketsStream) {
        try {
          System.out.println(parkingTicketsStream.available());
        }
        catch(IOException exception){
            return null;
        }
        return null;
    }
}