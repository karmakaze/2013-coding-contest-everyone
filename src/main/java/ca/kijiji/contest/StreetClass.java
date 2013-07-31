package ca.kijiji.contest;

import java.lang.Character;
import java.util.ArrayList;
import java.util.Arrays;
/**
 * Class used to extract just a street name from the location field given.
 * 
 * @author Eamonn Watson
 */

public class StreetClass {
    
    // Various Directions to exclude from the location field
    private static ArrayList<String> directions = new ArrayList<>();
    static {
        directions.add("N");
        directions.add("E");
        directions.add("S");
        directions.add("W");
        directions.add("NORTH");
        directions.add("EAST");
        directions.add("SOUTH");
        directions.add("WEST");
    }
    
    // Street Suffixes to exclude from the location field
    private static ArrayList<String> streetSuffix = new ArrayList<>();
    static {
        streetSuffix.add("AVE");
        streetSuffix.add("AVENUE");
        streetSuffix.add("AV");
        streetSuffix.add("BL");
        streetSuffix.add("BLV");
        streetSuffix.add("BLVD");
        streetSuffix.add("BOULEVARD");
        streetSuffix.add("BYWAY");
        streetSuffix.add("CIR");
        streetSuffix.add("CIRCLE");
        streetSuffix.add("COURT");
        streetSuffix.add("CRCL");
        streetSuffix.add("CIRL");
        streetSuffix.add("CL");
        streetSuffix.add("CR");
        streetSuffix.add("CRESCENT");
        streetSuffix.add("CRESENT");
        streetSuffix.add("CRES");
        streetSuffix.add("CRS");
        streetSuffix.add("CRT");
        streetSuffix.add("CT");
        streetSuffix.add("GARDEN");
        streetSuffix.add("GARDENS");
        streetSuffix.add("GRDNS");
        streetSuffix.add("GDS");
        streetSuffix.add("GATE");
        streetSuffix.add("GREEN");
        streetSuffix.add("GRV");
        streetSuffix.add("GT");
        streetSuffix.add("GDNS");
        streetSuffix.add("DR");
        streetSuffix.add("DRIVE");
        streetSuffix.add("LANE");
        streetSuffix.add("LANEWAY");
        streetSuffix.add("LN");
        streetSuffix.add("MEWS");
        streetSuffix.add("PALCE");
        streetSuffix.add("PATHWAY");
        streetSuffix.add("PARK");
        streetSuffix.add("PARKWAY");
        streetSuffix.add("PKWY");
        streetSuffix.add("PLACE");
        streetSuffix.add("PLAZA");
        streetSuffix.add("PLCE");
        streetSuffix.add("PL");
        streetSuffix.add("PK");
        streetSuffix.add("ROAD");
        streetSuffix.add("ROSEWAY");
        streetSuffix.add("RD");
        streetSuffix.add("SQ");
        streetSuffix.add("SQUARE");
        streetSuffix.add("ST");
        streetSuffix.add("STR");
        streetSuffix.add("STREET");
        streetSuffix.add("TER");
        streetSuffix.add("TERR");
        streetSuffix.add("TRAILWAY");
        streetSuffix.add("TRIALWAY");
        streetSuffix.add("TRL");
        streetSuffix.add("WALK");
        streetSuffix.add("WAY");
        streetSuffix.add("WY");
    }
    
    /**
     * Takes the location input from the file, parses it to remove just the street name.
     * 
     * @param location
     * @return the street name
     */    
    public static String getStreetName(String location){

        /**
         * 4 Possible fields, number, street name, street suffix, direction
         * Street names may be broken into multiple words, have to check for that.
         * Will have to make a list of possible street suffix's and directions to check against.
         * Determine if final location element is the direction
         */
        
        // Sanitize the location to remove characters that should not be in the address
        String locationToSanitize = location;
        String[] badChars = new String[]{"/","\\",".","?",",","\"","#","&","%","!","$","{","}","[","]"};
        for(String singleChar : badChars){
            locationToSanitize = locationToSanitize.replace(singleChar, "");
        }
        locationToSanitize = locationToSanitize.trim();
        if(locationToSanitize.equals("")){
            // location is empty so have to return something probably an error code..
            return "";
        }
        
        //split the location into segments
        String[] locsegs = locationToSanitize.split(" ");

        //Time to check the the 1st element to see if its a street number or a street name
        //Unfortunately since a street name can be 1ST ST, I cannot just check for numbers
        if (Character.isDigit(locsegs[0].charAt(0))) {
            //okay 1st element has a number, now need to determine what it is
            String[] numStreets = new String[]{"ST","ND","RD","TH"};
            boolean foundStreet = false;
            
            for (String strEnding : numStreets){
                if (locsegs[0].contains(strEnding)){
                    foundStreet = true;
                    break;
                }
            }
            
            if (!foundStreet) {
                // Okay so a number street name was not found, so its a street number, remove it.
                locsegs = Arrays.copyOfRange(locsegs, 1, locsegs.length);
            }
            
        }
        
        // if length is now zero then the entry was blank or JUST a number
        if (locsegs.length==0){ return ""; }
        
        //gonna have to work on segments from right to left
        //final section first to determine if its a direction or not        
        if (directions.contains(locsegs[locsegs.length-1])) {
            // final array element is a direction, discard it
            locsegs = Arrays.copyOf(locsegs, locsegs.length-1);
        }
        
        //time to see if the next one is a street suffix
        if (streetSuffix.contains(locsegs[locsegs.length-1])){
            // final array element is a suffix, discard it
            locsegs = Arrays.copyOf(locsegs, locsegs.length-1);
        }
        
        String outputStreet = "";
        //What is remaining must be the street name so time to join it all together into a string and output 
        for (String streets : locsegs){
           outputStreet += streets + " ";
        }
       
        return outputStreet.trim();
    }

}
