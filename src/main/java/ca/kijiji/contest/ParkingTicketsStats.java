package ca.kijiji.contest;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Map;
import java.util.SortedMap;

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
//        if num_location_parts == 5:
//            if location_parts[3] in SUFFIX_EQUIV_MAP.keys() and location_parts[4] in DIRECTION_EQUIV_MAP:
//                return '{} {}'.format(location_parts[1], location_parts[2])
//        if num_location_parts == 4:
//            if ' '.join(location_parts[1:]) == 'THE WEST MALL' or ' '.join(location_parts[1:]) == 'THE EAST MALL':
//                return ' '.join(location_parts[1:])
//            if location_parts[2] in SUFFIX_EQUIV_MAP.keys() and location_parts[3] in DIRECTION_EQUIV_MAP:
//                return '{}'.format(location_parts[1])
//            elif location_parts[3] in SUFFIX_EQUIV_MAP:
//                return '{} {}'.format(location_parts[1],
//                                      location_parts[2])
//            else:
//                raise Exception(location)
//        elif num_location_parts == 3:
//            if ' '.join(location_parts[1:]) == 'THE QUEENSWAY' or ' '.join(location_parts[1:]) == 'THE KINGSWAY':
//                return ' '.join(location_parts[1:])
//            try:
//                int(location_parts[0][0])
//                return '{}'.format(location_parts[1])
//            except:
//                return '{}'.format(location_parts[0])
//        elif num_location_parts == 2:
//            return '{}'.format(location_parts[0])
//        else:
//            return '{}'.format(location_parts[0])
//    except Exception as e:
//        # print 'Unable to parse: {} - {}'.format(location, e)
//        raise e
        
        return location;
    }
    
    // TODO: Move to own class?
    private static final Map<String, String> SUFFIX_EQUIV_MAP = null;
    static {
        Map<String, String> SUFFIX_EQUIV_MAP = new Hashtable<String, String>();
        SUFFIX_EQUIV_MAP.put("", "");
        SUFFIX_EQUIV_MAP.put("STREET", "STREET");
        SUFFIX_EQUIV_MAP.put("STR", "STREET");
        SUFFIX_EQUIV_MAP.put("AV", "AVENUE");
        SUFFIX_EQUIV_MAP.put("AVE", "AVENUE");
        SUFFIX_EQUIV_MAP.put("DR", "DRIVE");
        SUFFIX_EQUIV_MAP.put("DRIVE", "DRIVE");
        SUFFIX_EQUIV_MAP.put("LANE", "LANE");
        SUFFIX_EQUIV_MAP.put("LN", "LANE");
        SUFFIX_EQUIV_MAP.put("LA", "LANE");
        SUFFIX_EQUIV_MAP.put("CRES", "CRESCENT");
        SUFFIX_EQUIV_MAP.put("CRESCENT", "CRESCENT");
        SUFFIX_EQUIV_MAP.put("CR", "CRESCENT");
        SUFFIX_EQUIV_MAP.put("GDNS", "GARDENS");
        SUFFIX_EQUIV_MAP.put("GARDENS", "GARDENS");
        SUFFIX_EQUIV_MAP.put("SQ", "SQUARE");
        SUFFIX_EQUIV_MAP.put("SQUARE", "SQUARE");
        SUFFIX_EQUIV_MAP.put("RD", "ROAD");
        SUFFIX_EQUIV_MAP.put("ROAD", "ROAD");
        SUFFIX_EQUIV_MAP.put("BLVD", "BOULEVARD");
        SUFFIX_EQUIV_MAP.put("BL", "BOULEVARD");
        SUFFIX_EQUIV_MAP.put("BLV", "BOULEVARD");
        SUFFIX_EQUIV_MAP.put("BLVP", "BOULEVARD");
        SUFFIX_EQUIV_MAP.put("TER", "TERRACE");
        SUFFIX_EQUIV_MAP.put("TERRACE", "TERRACE");
        SUFFIX_EQUIV_MAP.put("GT", "GATE");
        SUFFIX_EQUIV_MAP.put("GATE", "GATE");
        SUFFIX_EQUIV_MAP.put("PARK", "PARK");
        SUFFIX_EQUIV_MAP.put("PK", "PARK");
        SUFFIX_EQUIV_MAP.put("QUAY", "QUAY");
        SUFFIX_EQUIV_MAP.put("COURT", "COURT");
        SUFFIX_EQUIV_MAP.put("CT", "COURT");
        SUFFIX_EQUIV_MAP.put("CRT", "COURT");
        SUFFIX_EQUIV_MAP.put("CIRCLE", "CIRCLE");
        SUFFIX_EQUIV_MAP.put("CIR", "CIRCLE");
        SUFFIX_EQUIV_MAP.put("CRCL", "CIRCLE");
        SUFFIX_EQUIV_MAP.put("CIRLE", "CIRCLE");
        SUFFIX_EQUIV_MAP.put("CIRL", "CIRCLE");
        SUFFIX_EQUIV_MAP.put("HILL", "HILL");
        SUFFIX_EQUIV_MAP.put("WAY", "WAY");
        SUFFIX_EQUIV_MAP.put("WY", "WAY");
        SUFFIX_EQUIV_MAP.put("PL", "PLACE");
        SUFFIX_EQUIV_MAP.put("PLACE", "PLACE");
        SUFFIX_EQUIV_MAP.put("TRAIL", "TRAIL");
        SUFFIX_EQUIV_MAP.put("TRL", "TRAIL");
        SUFFIX_EQUIV_MAP.put("GROVE", "GROVE");
        SUFFIX_EQUIV_MAP.put("GRV", "GROVE");
        SUFFIX_EQUIV_MAP.put("PARKWAY", "PARKWAY");
        SUFFIX_EQUIV_MAP.put("PKWY", "PARKWAY");
        SUFFIX_EQUIV_MAP.put("PATH", "PATH");
        SUFFIX_EQUIV_MAP.put("MEWS", "MEWS");
        SUFFIX_EQUIV_MAP.put("ROADWAY", "ROADWAY");
        SUFFIX_EQUIV_MAP.put("PATHWAY", "PATHWAY");
        SUFFIX_EQUIV_MAP.put("PTWY", "PATHWAY");
        SUFFIX_EQUIV_MAP.put("LOT", "LOT");
        SUFFIX_EQUIV_MAP.put("DONWAY", "DONWAY");
                
        SUFFIX_EQUIV_MAP = Collections.unmodifiableMap(SUFFIX_EQUIV_MAP);
    }

    private static final Map<String, String> DIRECTION_EQUIV_MAP = null;
    static {
        Map<String, String> DIRECTION_EQUIV_MAP = new Hashtable<String, String>();
        DIRECTION_EQUIV_MAP.put("", "");
        DIRECTION_EQUIV_MAP.put("NORTH", "NORTH");
        DIRECTION_EQUIV_MAP.put("N", "NORTH");
        DIRECTION_EQUIV_MAP.put("SOUTH", "SOUTH");
        DIRECTION_EQUIV_MAP.put("S", "SOUTH");
        DIRECTION_EQUIV_MAP.put("EAST", "EAST");
        DIRECTION_EQUIV_MAP.put("E", "EAST");
        DIRECTION_EQUIV_MAP.put("WEST", "WEST");
        DIRECTION_EQUIV_MAP.put("W", "WEST");

        DIRECTION_EQUIV_MAP = Collections.unmodifiableMap(DIRECTION_EQUIV_MAP);
    }
}
