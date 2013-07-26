package ca.kijiji.contest;

/**
 *
 * @author Eamonn
 */

public class StreetClass {
    
    public static String getStreetName(String location){

        // Sanitize the location to remove characters that should not be in the address
        String locationToSanitize = location;
        String[] badChars = new String[]{"/","\\",".","?",",","\"","#","&","%"};
        for(String singleChar : badChars){
            locationToSanitize = locationToSanitize.replace(singleChar, "");
        }

        locationToSanitize = locationToSanitize.trim();

        if(locationToSanitize == ""){
            // location is empty so
        }
        
        String[] locsegs = locationToSanitize.split(" ");
        
        /**
         * 4 Possible fields, number, street name, street suffix, direction
         * Street names may be broken into multiple words, have to check for that.
         * Will have to make a list of possible street suffix's and directions to check against.
         * Determine if final location element is the direction
         */
        
        
        
        return "";
    }

}
