package com.lishid.kijiji.contest.mapred;

import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.lishid.kijiji.contest.util.PseudoString;

public class Algorithm {
    
    private static WordFilter wordFilter = new WordFilter();
    private static int[] selectedColumns = new int[] { 4, 7, 0 };
    
    /**
     * The map process, reading lines and returning key-value pairs as Street name and Ticket amount
     * 
     * @param line the line from the csv file
     * @param result the result object to put values in
     */
    public static void map(PseudoString line, MapResult result) {
        if (line == null)
            return;
        PseudoString[] split = line.split(',', selectedColumns);
        result.value = split[0].toPositiveInteger();
        result.key = findRoadName(split[1]);
    }
    
    /**
     * Object to collect key-value pairs
     */
    public static class MapResult {
        public PseudoString key;
        public int value;
    }
    
    /**
     * Combine key-value pairs together by adding values together for the same key if it existed in the map
     * 
     * @param key
     * @param value
     * @param map the result collector
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
    
    private static PseudoString findRoadName(PseudoString roadName) {
        PseudoString[] tokens = roadName.split(' ');
        
        if (tokens.length == 1) {
            return roadName;
        }
        
        int startIndex = 0;
        int endIndex = tokens.length - 1;
        
        for (startIndex = 0; startIndex < endIndex; startIndex++) {
            PseudoString word = tokens[startIndex];
            // This will ignore all tokens with non-alphabetic words.
            // Note that all numbered streets will be gone,
            // but it's ok since there's practically no data with those
            // (only 4: 12TH=750$, 43RD=270$, 16TH=1645$, 42ND=40$)
            if (word.isAlphabetic()) {
                break;
            }
            
            continue;
        }
        
        for (endIndex = startIndex; endIndex < tokens.length - 1; endIndex++) {
            if (wordFilter.isWordFiltered(tokens[endIndex + 1])) {
                break;
            }
        }
        
        int start = roadName.start;
        int length = 0;
        for (int i = 0; i <= endIndex; i++) {
            if (i < startIndex) {
                start += tokens[i].length + 1;
            }
            else {
                length += tokens[i].length;
                if (i != endIndex) {
                    length += 1;
                }
            }
        }
        
        return new PseudoString(roadName.data, start, length);
    }
    
    private static class WordFilter {
        private Set<PseudoString> filteredWords = new HashSet<PseudoString>();
        
        public WordFilter() {
            addReservedFilteredWord();
        }
        
        public boolean isWordFiltered(PseudoString word) {
            return filteredWords.contains(word);
        }
        
        private void addReservedFilteredWord() {
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
        
        private void addFilteredWord(String... words) {
            for (int i = 0; i < words.length; i++) {
                filteredWords.add(new PseudoString(words[i]));
            }
        }
    }
}
