package ca.kijiji.contest;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.SortedMap;
import java.util.TreeMap;

import au.com.bytecode.opencsv.CSVReader;

public class ParkingTicketsStats {

	public static final String FINE_AMOUNT_FIELD_NAME = "set_fine_amount";
	public static final String LOCATION_FIELD_NAME = "location2";
	
    public static SortedMap<String, Integer> sortStreetsByProfitability(InputStream parkingTicketsStream) throws IOException {
		SortedMap<String, Integer> streetToFeeSum = new TreeMap<String, Integer>();
		
		CSVReader ticketReader = new CSVReader(new InputStreamReader(parkingTicketsStream));
		
		String[] ticketData = null;
		
		CSVNamedRow namedRow = new CSVNamedRow(ticketReader.readNext());
		System.out.println(namedRow);
		int counter = 0;
		while ((ticketData = ticketReader.readNext()) != null) {
			System.out.println(namedRow.getField(ticketData, LOCATION_FIELD_NAME));
			System.out.println(namedRow.getField(ticketData, FINE_AMOUNT_FIELD_NAME));
			if (counter == 15) {
				break;
			}
			++counter;
		}
		
		ticketReader.close();
//		int fineAmount = Integer.parseInt(ticket.get(4));
//		String location = ticket.get(7);
//		String street = StreetParser.parseStreet(location);
        return streetToFeeSum;
    }
}