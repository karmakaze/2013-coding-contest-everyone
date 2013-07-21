package ca.kijiji.contest;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.SortedMap;
import java.util.TreeMap;

import ca.kijiji.contest.exceptions.InvalidRowException;

import au.com.bytecode.opencsv.CSVReader;

public class ParkingTicketsStats {

	public static final String FINE_AMOUNT_FIELD_NAME = "set_fine_amount";
	public static final String LOCATION_FIELD_NAME = "location2";
	
    public static SortedMap<String, Integer> sortStreetsByProfitability(InputStream parkingTicketsStream) throws IOException {
		SortedMap<String, Integer> streetToFeeSum = new SortedCountMap<String, Integer>();
		
		CSVReader ticketReader = new CSVReader(new InputStreamReader(parkingTicketsStream));
		
		String[] ticketData = null;
		
		CSVNamedRow namedRow = new CSVNamedRow(ticketReader.readNext());
		while ((ticketData = ticketReader.readNext()) != null) {
			try {
				String location = namedRow.getField(ticketData, LOCATION_FIELD_NAME);
				int fineAmount = getFineAmount(namedRow, ticketData);
				String street = parseStreet(location);
				streetToFeeSum.put(street, fineAmount);
			}
			catch(InvalidRowException ire) {
				continue;
			}
		}
		ticketReader.close();
        return streetToFeeSum;
    }
    
    // TODO: Make these non-static functions and use instance vars.
    private static int getFineAmount(CSVNamedRow namedRow, String[] ticketData) throws InvalidRowException {
		int fineAmount = 0;
		try {
			fineAmount = namedRow.getIntegerField(ticketData, FINE_AMOUNT_FIELD_NAME);
		}
		catch(NumberFormatException nfe) {
			System.out.println(String.format("Invalid fee amount: %s", nfe.getMessage()));
			throw new InvalidRowException();
		}
		return fineAmount;    	    	
    }
    
    public static String parseStreet(String location) {
    	
    	// Special case for those weird streets.
        if (location == "THE QUEENSWAY" || location == "THE KINGSWAY" ||
                location == "THE WEST MALL" || location == "THE EAST MALL") {
                return location;
        }
        String[] location_parts = location.split(" ");
        int numLocationParts = location_parts.length;
        
        return location;
    }
}
