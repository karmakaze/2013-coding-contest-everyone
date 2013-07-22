package ca.kijiji.contest;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

public class CommonCalculations {
    
    /**
     * The map process, reading lines and returning key-value pairs from Street name and Ticket amount
     * 
     * @param line the line from the csv file
     * @param result the result object to put values in
     */
    public static void map(String line, MapResult result) {
        if (line == null)
            return;
        String[] split = line.split(",");
        result.key = findRoadName(split[7]);
        result.value = Integer.parseInt(split[4]);
    }
    
    /**
     * Object to collect key-value pairs
     */
    public static class MapResult {
        public String key;
        public int value;
    }
    
    /**
     * Used to partition map results to reducers
     * 
     * @param hashCode the hashcode of the key
     * @param partitions the number of partitions
     * @return the partition the key belongs to
     */
    public static int getPartition(int hashCode, int partitions) {
        return ((hashCode % partitions) + partitions) % partitions;
    }
    
    /**
     * Combine key-value pairs together
     * 
     * @param key
     * @param value
     * @param map to put in the result of the combination
     */
    public static void combine(String key, int value, Map<String, Integer> map) {
        if (map.containsKey(key)) {
            value += map.get(key);
        }
        map.put(key, value);
    }
    
    /**
     * Combine two maps
     * 
     * @param input to take values from
     * @param result to put the final results
     */
    public static void reduce(Map<String, Integer> input, Map<String, Integer> result) {
        for (Entry<String, Integer> entry : input.entrySet()) {
            combine(entry.getKey(), entry.getValue(), result);
        }
    }
    
    /**
     * Make a SortedMap by value
     * 
     * @param input
     * @return a SortedMap by value (Integer)
     */
    public static SortedMap<String, Integer> sort(Map<String, Integer> input) {
        TreeMap<String, Integer> result = new TreeMap<String, Integer>(new ValueComparator(input));
        result.putAll(input);
        return result;
    }
    
    private static String findRoadName(String roadName) {
        // Removed: Most data is upper case, lower case data is minimal
        // key = key.toUpperCase().trim();
        
        String[] tokens = roadName.split(" ");
        
        // Single word shortcut
        if (tokens.length == 1) {
            return roadName;
        }
        
        // Multi word
        int startIndex = 0;
        int endIndex = tokens.length - 1;
        // Seek starting index
        for (startIndex = 0; startIndex < endIndex; startIndex++) {
            String word = tokens[startIndex];
            // This will ignore all tokens with non-alphabetic characters
            if (isAlpha(word)) {
                break;
            }
            // Removed: There's practically no data starting with #TH as the street name
            // if(isNumberedStreetName(word)) { break; }
            
            continue;
        }
        // Seek ending index
        for (endIndex = startIndex; endIndex < tokens.length - 1; endIndex++) {
            if (filteredWords.contains(tokens[endIndex + 1])) {
                break;
            }
        }
        
        // Rebuild road name (from startIndex to endIndex)
        StringBuilder sb = new StringBuilder();
        for (int i = startIndex; i <= endIndex; i++) {
            if (tokens[i].isEmpty()) {
                continue;
            }
            sb.append(tokens[i]);
            if (i != endIndex) {
                sb.append(" ");
            }
        }
        return sb.toString();
    }
    
    /**
     * All words that should be filtered out, usually somewhere at the end of the road name
     */
    static Set<String> filteredWords = new HashSet<String>();
    static {
        addReservedFilteredWord();
    }
    
    private static void addReservedFilteredWord() {
        addFilteredWord("EAST", "WEST", "NORTH", "SOUTH");
        addFilteredWord("E", "W", "N", "S");
        addFilteredWord("AVENUE", "AVE", "AV");
        addFilteredWord("BOULEVARD", "BULVD", "BLVD", "BL");
        addFilteredWord("CIRCLE", "CIRCL", "CRCL", "CIRC", "CIR", "CR", "CIRCUIT");
        addFilteredWord("COURT", "CRT", "CRCT", "CT", "CTR");
        addFilteredWord("CRESCENT", "CRES", "CRE");
        addFilteredWord("DRIVE", "DR");
        addFilteredWord("EXPRESSWAY");
        addFilteredWord("GARDENS", "GRNDS", "GDNS", "GARDEN", "GDN");
        addFilteredWord("GATE", "GT");
        addFilteredWord("GREEN", "GRN");
        addFilteredWord("GROVE", "GRV");
        addFilteredWord("HEIGHTS", "HTS");
        addFilteredWord("HILL", "HILLS");
        addFilteredWord("LANE", "LN");
        addFilteredWord("LAWN", "LWN");
        addFilteredWord("LINE");
        addFilteredWord("MALL", "MEWS");
        addFilteredWord("PARKWAY", "PKWY", "PARK", "PARKING", "PK");
        addFilteredWord("PATHWAY", "PTWY", "PATH");
        addFilteredWord("PLACE", "PL");
        addFilteredWord("PROMENADE", "RAMP");
        addFilteredWord("ROAD", "RD", "ROADWAY", "RDWY");
        addFilteredWord("SQUARE", "SQ");
        addFilteredWord("STREET", "STR", "ST");
        addFilteredWord("TERRACE", "TER", "TERR", "TR");
        addFilteredWord("TRAIL", "TRL");
        addFilteredWord("VIEW", "WALKWAY", "WALK");
        addFilteredWord("WAYS", "WAY", "WY");
        addFilteredWord("WOODS");
    }
    
    private static void addFilteredWord(String... words) {
        filteredWords.addAll(Arrays.asList(words));
    }
    
    private static class ValueComparator implements Comparator<String> {
        Map<String, Integer> base;
        
        public ValueComparator(Map<String, Integer> base) {
            this.base = base;
        }
        
        // Note: this comparator imposes orderings that are inconsistent with equals.
        public int compare(String a, String b) {
            if (base.get(a) > base.get(b)) {
                return -1;
            }
            else if (base.get(a) < base.get(b)) {
                return 1;
            }
            else {
                // Returning 0 would merge keys
                return a.compareTo(b);
            }
        }
    }
    
    private static boolean isAlpha(String word) {
        if (word.isEmpty())
            return false;
        
        char[] chars = word.toCharArray();
        for (char c : chars) {
            if (!Character.isLetter(c)) {
                return false;
            }
        }
        
        return true;
    }
}
