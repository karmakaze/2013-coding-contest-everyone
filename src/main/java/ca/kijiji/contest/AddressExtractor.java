package ca.kijiji.contest;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * A class to extract the street name from a one line address of approximate form NUMBER STREET STREET_TYPE DIRECTION
 * @author djmorton
 */
public class AddressExtractor {
    //Most addresses will likely be a street number, followed by a one or two word street name followed by a variety of
    //suffixes.  We can reasonable consider only the first three white-space delimited tokens when determining the street
    //name, discarding the remainder, as the remainder is very likely one or more suffixes.
    private static final int MAX_TOKENS_TO_CONSIDER = 3;

    private static final int NO_TOKEN = -1;

    //Common address stop-words, variations and misspellings
    private static final Set<String> STOP_WORDS = new HashSet<String>(
            Arrays.asList(
                    "AE", "AV", "AVE.", "AVE", "AVENUE",
                    "BL", "BLD", "BLOUEVARD", "BLV", "BLVD.", "BLVD", "BOULEVARD", "BULV", "BULVD", "BVD", "BVLD",
                    "CIR", "CIRC", "CIRCL", "CIRCLE", "CIRL", "CIRLE", "COURT", "CR", "CRCL", "CRE",
                    "CRES.", "CRES", "CRESC.", "CRESC", "CRESCENT", "CRESENT", "CREST", "CRS", "CRST",
                    "CRT.", "CRT", "CT.", "CT",
                    "DR.", "DR", "DRIVE",
                    "GARDEN", "GARDENS", "GDN", "GDNS", "GDS", "GR", "GRD", "GRDN", "GRDNS", "GROVE", "GRV", "GT",
                    "HTS", "LA", "LANE", "LANEWAY", "LN", "LWN",
                    "MEWS",
                    "PARK", "PARKD", "PARKWAY",
                    "PATH", "PATHWAY",
                    "PL.", "PL", "PLACE", "PLCE",
                    "PK.", "PK", "PKWY", "PRK", 
                    "RD.", "RD", "RDWY", "ROAD", "ROADWAY",
                    "SQ",
                    "ST", "STE", "STEET", "STR", "STREET",
                    "TER", "TERR",
                    "TR", "TRAIL", "TRL",
                    "WAY", "WY",
                    "NORTH", "EAST", "SOUTH", "WEST"
                    ));

    /**
     * Make a best-effort guess as to the portion of the parameter string that represents the street name and return that name
     * @param address A string representing an address in the approximate form NUMBER STREET STREET_TYPE DIRECTION 
     * @return The likely name of the street contained in the supplied address parameter
     */
    public String extractStreetFromAddress(final String address) {
        final String[] addressTokens = address.split(" ");
        final int[] tokenLength = new int[MAX_TOKENS_TO_CONSIDER]; 
        int tokenIndex = 0;

        int firstToken = NO_TOKEN;
        int lastToken = NO_TOKEN;

        //Iterate through all the tokens until we have considered MAX_TOKENS_TO_CONSIDER tokens.
        //Determine the first and last token that is a likely candidate for the street name.
        for(String token : addressTokens) {
            if (tokenIndex >= MAX_TOKENS_TO_CONSIDER) {
                break;
            }
            //Store the length of each token we consider
            tokenLength[tokenIndex] = token.length();

            //Determine if we can disregard the token or if it is likely part of the address
            if (shouldDisregardToken(token, tokenIndex)) {
                if (firstToken != NO_TOKEN) {
                    //If we have found a token we can disregard and we have already found at
                    //least one token we will not disregard, we have likely found the street name as
                    //any further tokens are likely suffixes we need not consider.
                    break;  
                } else {
                    tokenIndex++;
                    continue;
                }
            }

            if (firstToken == NO_TOKEN) {
                firstToken = tokenIndex;
            }
            lastToken = tokenIndex;
            tokenIndex++;
        }

        if (firstToken == NO_TOKEN) {
            //We didn't find a viable token representing the street name in the first
            //MAX_TOKENS_TO_CONSIDER tokens.  The address line is likely garbage and
            //can be discarded.
            return null;
        }

        //Calculate the indexes of those tokens that are likely members in the street name in the
        //parameter address and return the substring of the address containing that street name
        //Note:  Calculating indexes in this manner makes the code marginally more difficult to
        //read (compared to simply using a StringBuilder to build the address string) but we do
        //get a performance boost using the indexes directly.
        int startIndex = 0;
        int endIndex = 0;
        for(int i = 0; i < firstToken; i++) {
            startIndex += (tokenLength[i] + 1);
            endIndex = startIndex + tokenLength[i + 1];
        }

        for(int i = firstToken + 1; i <= lastToken; i++) {
            endIndex += (tokenLength[i] + 1);
        }

        return address.substring(startIndex, endIndex);
    }
    
    private boolean shouldDisregardToken(final String token, final int tokenIndex) {
        return token.length() <= 1                        // Any single character token is very likely not a 
                                                          // key part of the address and can be discarded

                || isFirstCharADigit(token)               // Any token beginning with a number is very likely a
                                                          // Street number and can be discarded

                || (tokenIndex > 1 && isStopWord(token)); // Any token that is a stop word and doesn't appear in the
                                                          // first two positions of the address should be discarded.
                                                          // Stop words are almost always found towards the end of 
                                                          // addresses and could in some cased be mistaken for real
                                                          // parts of the address towards the beginning 
                                                          // (ST as Saint and not Street, etc.)
    }

    private boolean isFirstCharADigit(final String string) {
        return Character.isDigit(string.charAt(0));
    }

    private boolean isStopWord(final String string) {
        return STOP_WORDS.contains(string);
    }
}
