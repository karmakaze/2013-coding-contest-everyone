package ca.kijiji.contest;

import java.io.InputStream;
import java.util.SortedMap;

import ca.kijiji.contest.mapred.MapReduceProcessor;

public class ParkingTicketsStats {
    
    public static SortedMap<String, Integer> sortStreetsByProfitability(InputStream parkingTicketsStream) throws Exception {
        // return new JfuerthSingleThreadedProcessor().sortStreetsByProfitability(parkingTicketsStream);
        // return new SingleThreadedProcessor().sortStreetsByProfitability(parkingTicketsStream);
        return new MapReduceProcessor().sortStreetsByProfitability(parkingTicketsStream);
    }
}