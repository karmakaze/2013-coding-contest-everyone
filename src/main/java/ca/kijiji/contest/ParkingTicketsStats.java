package ca.kijiji.contest;

import java.io.InputStream;
import java.util.SortedMap;

/**
 * Pipeline decomposition:
 * - InputStream
 * - byte[] blocks
 * - lines
 * - fields (e.g. fine, location)
 * - parse fine, location
 * - hash location
 *
 */
public class ParkingTicketsStats {
	public final static int algorithm = 2;

    public static SortedMap<String, Integer> sortStreetsByProfitability(final InputStream parkingTicketsStream) {
    	switch (algorithm) {
    	case 2: return ParkingTicketsStats2.sortStreetsByProfitability(parkingTicketsStream);

    	case 3: return ParkingTicketsStats3.sortStreetsByProfitability(parkingTicketsStream);

        default: throw new IllegalStateException("Selected algorithm "+ algorithm +" has no implementation.");
    	}
    }
}