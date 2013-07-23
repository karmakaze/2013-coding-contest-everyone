package ca.kijiji.contest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;


/**
 * A helper class to map the suffixes and directions into 1 canonical form for comparison.
 */
public class SuffixDirectionEquilizer {

    private static Map<String, String> SUFFIX_EQUIV_MAP = null;
    static {
        SUFFIX_EQUIV_MAP = new Hashtable<String, String>();
        SUFFIX_EQUIV_MAP.put("", "");
        SUFFIX_EQUIV_MAP.put("STREET", "STREET");
        SUFFIX_EQUIV_MAP.put("STR", "STREET");
        SUFFIX_EQUIV_MAP.put("ST", "STREET");
        SUFFIX_EQUIV_MAP.put("AV", "AVENUE");
        SUFFIX_EQUIV_MAP.put("AVE", "AVENUE");
        SUFFIX_EQUIV_MAP.put("AVENUE", "AVENUE");
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
        SUFFIX_EQUIV_MAP.put("QUY", "QUAY");
        SUFFIX_EQUIV_MAP.put("COURT", "COURT");
        SUFFIX_EQUIV_MAP.put("CT", "COURT");
        SUFFIX_EQUIV_MAP.put("CRT", "COURT");
        SUFFIX_EQUIV_MAP.put("CIRCLE", "CIRCLE");
        SUFFIX_EQUIV_MAP.put("CIR", "CIRCLE");
        SUFFIX_EQUIV_MAP.put("CRCL", "CIRCLE");
        SUFFIX_EQUIV_MAP.put("CIRLE", "CIRCLE");
        SUFFIX_EQUIV_MAP.put("CIRL", "CIRCLE");
        SUFFIX_EQUIV_MAP.put("CIRC", "CIRCLE");
        SUFFIX_EQUIV_MAP.put("CRCLE", "CIRCLE");
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

    private static Map<String, String> DIRECTION_EQUIV_MAP = null;
    static {
        DIRECTION_EQUIV_MAP = new Hashtable<String, String>();
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
    
    private static List<String> NUMBERED_STREET_ENDINGS = null;
    static {
    	NUMBERED_STREET_ENDINGS = new ArrayList<String>();
    	NUMBERED_STREET_ENDINGS.add("ST");
    	NUMBERED_STREET_ENDINGS.add("ND");
    	NUMBERED_STREET_ENDINGS.add("RD");
    	NUMBERED_STREET_ENDINGS.add("TH");
    }
    
    /**
     * Determines if a string is a suffix.
     * @param s The string that may or may not be a suffix.
     * @return True if it is a sort of suffix. False otherwise.
     */
    public static boolean isSuffix(String s) {
    	return SUFFIX_EQUIV_MAP.containsKey(s);
    }

    /**
     * Determines if a string is a direction.
     * @param s The string that may or may not be a direction.
     * @return True if it is a sort of direction. False otherwise.
     */
    public static boolean isDirection(String s) {
    	return DIRECTION_EQUIV_MAP.containsKey(s);
    }    

    /**
     * Determines if the string is a numbered street.
     * @param s The string to test.
     * @return True if it's a numbered street. False if it ain't.
     */
    public static boolean isNumberedStreet(String s) {
    	boolean startsWithNumber = Character.isDigit(s.charAt(0));
    	
    	boolean numberedEnding = false;
    	for (String ending : NUMBERED_STREET_ENDINGS) {
    		if (s.endsWith(ending)) {
    			numberedEnding = true;
    		}
    	}
    	
    	return startsWithNumber && numberedEnding;
    	
    }
}
