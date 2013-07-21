package ca.kijiji.contest;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.SortedMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.kijiji.contest.exceptions.InvalidRowException;
import ca.kijiji.contest.exceptions.UnparseableLocationException;

import au.com.bytecode.opencsv.CSVReader;

public class ParkingTicketsStats {

	private static final Logger LOG = LoggerFactory.getLogger(ParkingTicketsStats.class);
	
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
			catch(UnparseableLocationException ule) {
				LOG.info(ule.toString());
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
        
        // TODO: What if 2 word street and suffix?
        
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
        else {
        	throw new UnparseableLocationException(location);
        }
    }
    
}
