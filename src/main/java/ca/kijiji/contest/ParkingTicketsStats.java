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
		SortedMap<String, Integer> streetToFeeSum = new TreeMap<String, Integer>();
		
		CSVReader ticketReader = new CSVReader(new InputStreamReader(parkingTicketsStream));
		
		String[] ticketData = null;
		
		CSVNamedRow namedRow = new CSVNamedRow(ticketReader.readNext());
		while ((ticketData = ticketReader.readNext()) != null) {
			try {
				String location = namedRow.getField(ticketData, LOCATION_FIELD_NAME);
				int fineAmount = getFineAmount(namedRow, ticketData);
			}
			catch(InvalidRowException ire) {
				continue;
			}
		}
		ticketReader.close();
//		String street = StreetParser.parseStreet(location);
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
}
