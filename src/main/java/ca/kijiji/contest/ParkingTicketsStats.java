package ca.kijiji.contest;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.SortedMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.csvreader.CsvReader;

import ca.kijiji.contest.exceptions.UnparseableLocationException;


public class ParkingTicketsStats {

	private static final Logger LOG = LoggerFactory.getLogger(ParkingTicketsStats.class);
	public static final String FINE_AMOUNT_FIELD_NAME = "set_fine_amount";
	public static final String LOCATION_FIELD_NAME = "location2";


    public static SortedMap<String, Integer> sortStreetsByProfitability(InputStream parkingTicketsStream) throws IOException {
    	
    	SortedMap<String, Integer> streetToFineSum = new SortedCountMap<String, Integer>();
		CsvReader ticketReader = new CsvReader(parkingTicketsStream, Charset.defaultCharset());
		
		String[] ticketData = null;

		// TODO: Make Iterator for CSVReader?
		ticketReader.readRecord();
		CSVNamedRow namedRow = new CSVNamedRow(ticketReader.getValues());		
		
		while (ticketReader.readRecord()) {
			try {
				ticketData = ticketReader.getValues();
				String location = namedRow.getField(ticketData, LOCATION_FIELD_NAME);
				int fineAmount = namedRow.getIntegerField(ticketData, FINE_AMOUNT_FIELD_NAME);
				String street = parseStreet(location);
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
    
    public static String parseStreet(String location) throws UnparseableLocationException {
    	
    	// Special case for those weird streets.
        if (location == "THE QUEENSWAY" || location == "THE KINGSWAY" ||
                location == "THE WEST MALL" || location == "THE EAST MALL") {
                return location;
        }
        String[] locationParts = location.split(" ");
        int numLocationParts = locationParts.length;

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
												 Character.isDigit(locationParts[0].charAt(0)) &&
        										 SuffixDirectionEquilizer.isSuffix(locationParts[2]) &&
        										 SuffixDirectionEquilizer.isDirection(locationParts[3]);
        
        boolean hasNumTwoWordStreetAndSuffix = numLocationParts == 4 &&
        									   SuffixDirectionEquilizer.isSuffix(locationParts[3]);
        
        boolean hasTwoWordStreetSuffixAndDirection = numLocationParts == 4 &&
        											 !Character.isDigit(locationParts[0].charAt(0)) &&
        											 SuffixDirectionEquilizer.isSuffix(locationParts[2]) &&
            										 SuffixDirectionEquilizer.isDirection(locationParts[3]);
        
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
        boolean hasStreetAndSuffix = numLocationParts == 2 &&
        							 SuffixDirectionEquilizer.isSuffix(locationParts[1]);
        
        boolean hasTwoWordStreetOnly = numLocationParts == 2 &&
        							   !SuffixDirectionEquilizer.isSuffix(locationParts[1]);
        
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
        else if (hasTwoWordStreetSuffixAndDirection || hasTwoWordStreetOnly || hasTwoWordStreetAndSuffix) {
        	return String.format("%s %s", locationParts[0], locationParts[1]);
        }
        else {
        	throw new UnparseableLocationException(location);
        }
    }

}
