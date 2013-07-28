package com.lishid.kijiji.contest.mapred;

import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.lishid.kijiji.contest.util.MutableInteger;
import com.lishid.kijiji.contest.util.MutableString;

public class Algorithm {
    
    private static SuffixFilter suffixFilter = new SuffixFilter();
    
    /**
     * The map process, reading lines and returning key-value pairs as Street name and Ticket amount
     * 
     * @param line the line from the csv file
     * @param result the result object to put values in
     */
    public static void map(MutableString line, MapResult result) {
        if (line == null)
            return;
        
        // Read from back to front as we don't really care about the first 6 columns
        int wordEnd = line.end;
        int col = 0;
        int roadNameStart = 0;
        int roadNameLength = 0;
        int fineAmountStart = 0;
        int fineAmountLength = 0;
        for (int i = line.end - 1; i >= line.start; i--) {
            if (line.data[i] == ',') {
                // Column 11 => Road Name
                if (col == 3) {
                    roadNameStart = i + 1;
                    roadNameLength = wordEnd - i - 1;
                }
                // Column 7 => Fine Amount
                else if (col == 6) {
                    fineAmountStart = i + 1;
                    fineAmountLength = wordEnd - i - 1;
                    break;
                }
                wordEnd = i;
                col++;
            }
        }
        result.value = MutableString.toPositiveInteger(line.data, fineAmountStart, fineAmountLength);
        // MutableString line can now be recycled as it is no longer used
        result.key = findRoadName(line.useAsNewString(line.data, roadNameStart, roadNameLength));
    }
    
    /**
     * Object to collect key-value pairs
     */
    public static class MapResult {
        public MutableString key;
        public int value;
    }
    
    /**
     * Combine key-value pairs together by adding values together for the same key if it existed in the map
     * 
     * @return a MutableInteger to be recycled
     */
    public static MutableInteger combine(MutableString key, MutableInteger value, Map<MutableString, MutableInteger> map) {
        MutableInteger previousValue = map.get(key);
        if (previousValue != null) {
            previousValue.add(value.value);
            return value;
        }
        else {
            map.put(key, value);
        }
        
        return null;
    }
    
    /**
     * Combine two maps
     * 
     * @param input to take values from
     * @param result to put the final results
     */
    public static void reduce(Map<MutableString, MutableInteger> input, Map<MutableString, MutableInteger> result) {
        for (Entry<MutableString, MutableInteger> entry : input.entrySet()) {
            combine(entry.getKey(), entry.getValue(), result);
        }
    }
    
    /**
     * This method has been optimized to cut off the front and back of the given un-formatted road name.
     * Using direct access to the backed array of the MutableString, this method can quickly scan through characters
     */
    private static MutableString findRoadName(MutableString roadName) {
        int startIndex;
        int endIndex = roadName.end;
        int wordStart = roadName.start;
        
        // Step 1, skip all non-alphabetic words
        boolean alphabetic = true;
        boolean wordStarted = false;
        for (startIndex = roadName.start; startIndex < endIndex; startIndex++) {
            if (roadName.data[startIndex] == ' ') {
                // Found alphabetic word
                if (wordStarted && alphabetic) {
                    break;
                }
                // Reset
                wordStarted = false;
                alphabetic = true;
                wordStart = startIndex + 1;
            }
            else {
                wordStarted = true;
                if (alphabetic && (roadName.data[startIndex] < 'A' || roadName.data[startIndex] > 'Z')) {
                    alphabetic = false;
                }
            }
        }
        
        // To avoid creating a new object, use the same object as temporary string
        roadName.backup();
        
        // Step 2, find first filtered suffix
        int endWordStart = startIndex + 1;
        for (endIndex = startIndex + 1; endIndex < roadName.end; endIndex++) {
            if (roadName.data[endIndex] == ' ') {
                boolean foundSuffix = suffixFilter.isWordFilteredSuffix(roadName.useAsNewString(roadName.data, endWordStart, endIndex - endWordStart));
                roadName.restore();
                if (foundSuffix) {
                    break;
                }
                endWordStart = endIndex + 1;
            }
        }
        
        // Construct new word by only cutting off the front and back
        int length = endWordStart - wordStart - 1;
        if (length == roadName.length) {
            return roadName;
        }
        
        // MutableString roadName is no longer needed, recycle as new string
        return roadName.useAsNewString(roadName.data, wordStart, length);
    }
    
    private static class SuffixFilter {
        private Set<MutableString> filteredSuffixes = new HashSet<MutableString>();
        
        public SuffixFilter() {
            addAllFilteredSuffix();
        }
        
        public boolean isWordFilteredSuffix(MutableString word) {
            return filteredSuffixes.contains(word);
        }
        
        private void addAllFilteredSuffix() {
            addFilteredSuffix("EAST", "WEST", "NORTH", "SOUTH");
            addFilteredSuffix("E", "W", "N", "S");
            addFilteredSuffix("AVENUE", "AVE", "AV");
            addFilteredSuffix("BOULEVARD", "BULVD", "BLVD", "BL");
            addFilteredSuffix("CIRCLE", "CIRCL", "CRCL", "CIRC", "CIR", "CR", "CIRCUIT");
            addFilteredSuffix("COURT", "CRT", "CRCT", "CT", "CTR");
            addFilteredSuffix("CRESCENT", "CRES", "CRE");
            addFilteredSuffix("DRIVE", "DR");
            addFilteredSuffix("EXPRESSWAY");
            addFilteredSuffix("GARDENS", "GRNDS", "GDNS", "GARDEN", "GDN");
            addFilteredSuffix("GATE", "GT");
            addFilteredSuffix("GREEN", "GRN");
            addFilteredSuffix("GROVE", "GRV");
            addFilteredSuffix("HEIGHTS", "HTS");
            addFilteredSuffix("HILL", "HILLS");
            addFilteredSuffix("LANE", "LN");
            addFilteredSuffix("LAWN", "LWN");
            addFilteredSuffix("LINE");
            addFilteredSuffix("MALL", "MEWS");
            addFilteredSuffix("PARKWAY", "PKWY", "PARK", "PARKING", "PK");
            addFilteredSuffix("PATHWAY", "PTWY", "PATH");
            addFilteredSuffix("PLACE", "PL");
            addFilteredSuffix("PROMENADE", "RAMP");
            addFilteredSuffix("ROAD", "RD", "ROADWAY", "RDWY");
            addFilteredSuffix("SQUARE", "SQ");
            addFilteredSuffix("STREET", "STR", "ST");
            addFilteredSuffix("TERRACE", "TER", "TERR", "TR");
            addFilteredSuffix("TRAIL", "TRL");
            addFilteredSuffix("VIEW", "WALKWAY", "WALK");
            addFilteredSuffix("WAYS", "WAY", "WY");
            addFilteredSuffix("WOODS");
        }
        
        private void addFilteredSuffix(String... words) {
            for (int i = 0; i < words.length; i++) {
                filteredSuffixes.add(new MutableString(words[i]));
            }
        }
    }
}
