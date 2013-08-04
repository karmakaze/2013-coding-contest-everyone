package ca.kijiji.contest;

import java.io.InputStream;
import java.util.SortedMap;

import ca.kijiji.contest.ParkingTicketsStatsApp;

public class ParkingTicketsStats {

    public static SortedMap<String, Integer> sortStreetsByProfitability(InputStream parkingTicketsStream) {
	    return ParkingTicketsStatsApp.getSortedMap(parkingTicketsStream);
    }
}
