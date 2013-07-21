package ca.kijiji.contest;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.SortedMap;

import com.csvreader.CsvReader;

import ca.kijiji.contest.exceptions.UnparseableLocationException;

// import au.com.bytecode.opencsv.CSVReader;

public class ParkingTicketsStats {

	public static final String FINE_AMOUNT_FIELD_NAME = "set_fine_amount";
	public static final String LOCATION_FIELD_NAME = "location2";


    public static SortedMap<String, Integer> sortStreetsByProfitability(InputStream parkingTicketsStream) throws IOException {    	
    	SortedMap<String, Integer> streetToFineSum = new SortedCountMap<String, Integer>();
		CsvReader ticketReader = new CsvReader(parkingTicketsStream, Charset.defaultCharset());
		
		String[] ticketData = null;

		// TODO: Make util to get fields by name.
		// TODO: Catch exception when parsing int fails.		
		ticketReader.readRecord();
		
		while (ticketReader.readRecord()) {
			try {
				ticketData = ticketReader.getValues();
				String location = ticketData[7];				
				int fineAmount = Integer.parseInt(ticketData[4]);
				String street = parseStreet(location);
  				streetToFineSum.put(street, fineAmount);
			}
			catch(UnparseableLocationException ule) {
				continue;
			}
		}
		ticketReader.close();
        return streetToFineSum;
    }
    
    public static String parseStreet(String location) throws UnparseableLocationException {
    	
    	// Special case for those weird streets.
        if (location == "THE QUEENSWAY" || location == "THE KINGSWAY" ||
                location == "THE WEST MALL" || location == "THE EAST MALL") {
                return location;
        }
        String[] locationParts = location.split(" ");
        int numLocationParts = locationParts.length;

        // TODO: Go through cases and make sure you take into account 2 word streets.
        // 5 parts
        boolean hasAllPartsPlusTwoWordStreetName = numLocationParts == 5 &&
        										   SuffixDirectionEquilizer.isSuffix(locationParts[3]) &&
        										   SuffixDirectionEquilizer.isDirection(locationParts[4]);
        
        // 4 parts
        boolean isAMall = numLocationParts == 4 &&
		          (String.format("%s %s %s", locationParts[1], locationParts[2], locationParts[3])
		           .equals("THE EAST MALL") ||
		           String.format("%s %s %s", locationParts[1], locationParts[2], locationParts[3])
		           .equals("THE WEST MALL"));

		boolean hasNumStreetSuffixAndDirection = numLocationParts == 4 && 
        										 SuffixDirectionEquilizer.isSuffix(locationParts[2]) &&
        										 SuffixDirectionEquilizer.isDirection(locationParts[3]);
        
        boolean hasNumTwoWordStreetAndSuffix = numLocationParts == 4 &&
        									   SuffixDirectionEquilizer.isSuffix(locationParts[3]);
        
      
        // 3 parts
        boolean hasNumAndSpecialWays = numLocationParts == 3 &&
        							   (String.format("%s %s", locationParts[1], locationParts[2]).equals("THE QUEENSWAY") ||
        							    String.format("%s %s", locationParts[1], locationParts[2]).equals("THE KINGSWAY"));
        
        boolean hasNumStreetAndSuffix = numLocationParts == 3 &&
        								Character.isDigit(locationParts[0].charAt(0));
        
        boolean hasStreetSuffixAndDirection = numLocationParts == 3 &&
											  !Character.isDigit(locationParts[0].charAt(0));
        
        boolean hasTwoWordStreetAndSuffix = numLocationParts == 3 &&
        								    !Character.isDigit(locationParts[0].charAt(0)) &&
        									SuffixDirectionEquilizer.isSuffix(locationParts[2]); 
        
        // 2 parts
        boolean hasStreetAndSuffix = numLocationParts == 2;
        
        // TODO: What if 2 word street and nothing else?
        
        // 1 part
        boolean hasStreetOnly = numLocationParts == 1;
        
        if (hasAllPartsPlusTwoWordStreetName) {        	
    		// Get the 2 word street name
    		return String.format("%s %s", locationParts[1], locationParts[2]);
    	}
        else if (isAMall) {
        	return String.format("%s %s %s", locationParts[1], locationParts[2], locationParts[3]);
        }
        else if (hasNumStreetSuffixAndDirection) {
        	return locationParts[1];
        }
        else if (hasNumTwoWordStreetAndSuffix || hasNumAndSpecialWays) {
        	return String.format("%s %s", locationParts[1], locationParts[2]);
        }
        else if (hasNumStreetAndSuffix) {
        	return locationParts[1];
        }
        else if (hasStreetSuffixAndDirection || hasStreetAndSuffix || hasStreetOnly) {
        	return locationParts[0];
        }
        else if (hasTwoWordStreetAndSuffix) {
        	return String.format("%s %s", locationParts[0], locationParts[1]);
        }
        else {
        	throw new UnparseableLocationException(location);
        }
    }

}
