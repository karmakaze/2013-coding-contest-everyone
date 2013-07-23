package ca.kijiji.contest;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.SortedMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.csvreader.CsvReader;

import ca.kijiji.contest.exceptions.UnparseableLocationException;


/**
 * Takes in a CSV stream and returns an immutable map that maps street to sum of fines for that street.
 * The map is in descending order from the largest fine sum to the smallest.
 *
 */
public class ParkingTicketsStats {

	private static final Logger LOG = LoggerFactory.getLogger(ParkingTicketsStats.class);
	public static final String FINE_AMOUNT_FIELD_NAME = "set_fine_amount";
	public static final String LOCATION_FIELD_NAME = "location2";

	/**
	 * Returns the map of streets to fines.
	 * @param parkingTicketsStream Input CSV stream.
	 * @return The sorted map of street to fines.
	 * @throws IOException Error reading file.
	 */
    public static SortedMap<String, Integer> sortStreetsByProfitability(InputStream parkingTicketsStream) throws IOException {
    	
    	SortedMap<String, Integer> streetToFineSum = new SortedCountMap<String, Integer>();
		CsvReader ticketReader = new CsvReader(parkingTicketsStream, Charset.defaultCharset());
		
		String[] ticketData = null;

		ticketReader.readRecord();
		CSVNamedRow namedRow = new CSVNamedRow(ticketReader.getValues());		
		
		while (ticketReader.readRecord()) {
			try {
				ticketData = ticketReader.getValues();
				String location = namedRow.getField(ticketData, LOCATION_FIELD_NAME);
				int fineAmount = namedRow.getIntegerField(ticketData, FINE_AMOUNT_FIELD_NAME);
				String street = StreetUtil.parseStreet(location);				
  				streetToFineSum.put(street, fineAmount);
			}
			catch(UnparseableLocationException ule) {
				LOG.warn(ule.toString());
				continue;
			}
			catch (NumberFormatException nfe) {
				LOG.warn(nfe.toString());				
				continue;
			}				
		}
		ticketReader.close();
        return new ImmutableSortedByValueMap(streetToFineSum);
    }        
}
