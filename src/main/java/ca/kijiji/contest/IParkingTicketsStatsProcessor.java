package ca.kijiji.contest;

import java.io.InputStream;
import java.util.SortedMap;

public interface IParkingTicketsStatsProcessor
{
    public SortedMap<String, Integer> sortStreetsByProfitability(InputStream inputStream) throws Exception;
}
