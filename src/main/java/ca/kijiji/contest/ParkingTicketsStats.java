package ca.kijiji.contest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.SortedMap;
import java.util.TreeMap;

public final class ParkingTicketsStats {
    private static final Logger LOG = LoggerFactory.getLogger(ParkingTicketsStats.class);

    public static SortedMap<String, Integer> sortStreetsByProfitability(InputStream parkingTicketsStream) {
        if (parkingTicketsStream == null){
            LOG.error("Input stream is Null");
            return new TreeMap<>();
        }

        final DataParser parser = new DataParser(parkingTicketsStream);
        try {
            return parser.parse();
        } catch (IOException e) {
            LOG.error("Can't parse input data", e);
        }

        return new TreeMap<>();
    }
}