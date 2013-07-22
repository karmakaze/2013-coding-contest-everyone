package ca.kijiji.contest;

import java.io.InputStream;
import java.util.SortedMap;

/**
 * Interface used to test out different solutions
 * 
 * @author lishid
 */
public interface IParkingTicketsStatsProcessor {
    public SortedMap<String, Integer> sortStreetsByProfitability(InputStream inputStream) throws Exception;
}
