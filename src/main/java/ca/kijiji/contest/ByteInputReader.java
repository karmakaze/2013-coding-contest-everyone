package ca.kijiji.contest;


/**
 * This class mimicks the features of an InputStreamReader working on a byte[] buffer.
 * Instead of reading lines, it extracts Street objects from the data, one per line. 
 * The street name is cleaned stripping away the number and any trailing information, without using
 * strings. Strings are created only when the location2 field has been cleaned. 
 * Profit is decoded and a Street object is extracted only if the street name has non-zero length and
 * the profit is greater than 0.
 * It's a bit complicated, but waaaaaay faster than working on strings and/or stringbuilders
 * @author Chiara
 */
final class ByteInputReader {

    /**
     * Position of the profit field in the record
     */
    final static int PROFIT_LOC = 4;
    final static int RECORD_LENGTH = 11;
    /**
     * Position of the streetname field in the record
     */
    final static int STREETNAME_LOC = 7;

    byte[] input;
    int position; // Current position in the input (pointer)
    int end;
    int extractedLines = 0;

    public boolean eof() {
	return position >= end;
    }

    /**
     * Extract streetname and profit information from a line. Information is extracted from fields identified by
     * {@link #STREETNAME_LOC} and {@link #PROFIT_LOC}
     * 
     * @param line
     * @return a StreetMap.Street object with the streetname as extracted from the line (non-normalized) and the profit
     *         parsed into an int.
     */
    private final static char SEPARATOR = ',';
    private final static char EOL = '\n';

    public Street extract() {
	int step = 1;
	int lastSplit = -1;
	int location = 0;
	String streetname = null;
	int profit = -1;
	int readBytes = 0;
	boolean isEol = false;
	for (int i = position; !isEol && i < end; i += step) {
	    boolean split = false;
	    char c = (char) input[i];
	    isEol = c == EOL;
	    split = isEol || c == SEPARATOR;
	    if (split) {
		int start = lastSplit + step;
		int end = i;
		if (location == PROFIT_LOC) {
		    profit = decodeInt(input, start, end - start);
		} else if (location == STREETNAME_LOC) {
		    streetname = getCleanStreetName(input, start, end - start);
		}
		location++;
		lastSplit = i;
	    }
	    readBytes = i - position;
	}
	try {
	    if (isEol && location == RECORD_LENGTH && streetname.length() > 0 && profit > 0) {
		return new Street(streetname, profit);
	    } else {
		return null;
	    }
	} finally {
	    position = position + readBytes + 1;
	}
    }

    private static boolean match(byte[] buf, int start, int len, String string) {
        for (int i = 0; i < len; i++) {
            if (buf[start + i] != string.charAt(i)) {
        	return false;
            }
        }
        return true;
    }

    /**
     * Removes clutter from the specified portion of the buffer and returns a String
     * @param buf
     * @param offset
     * @param len
     * @return the street names, stripped of number, illegal chars "AND "s and " ST", 
     *    and leading and trailing whitespaces
     */
    public final static String getCleanStreetName(byte[] buf, int offset, int len) {
    	int start = offset;
    	int end = start + len;
    	int whitespaceCount = 0;
    	start = trimStart(buf, start, len);
    	len = end - start;
    	end = trimEnd(buf, start, len);
    	len = end - start;
    
    	if (len > 4 && match(buf, start, 4, "AND ")) {
    	    start += 4;
    	    len = end - start;
    	}
    
    	// Remove isolated letters at the beginning of the word
    	if (len > 1 && buf[start + 1] == ' ') {
    	    start++;
    	    len--;
    	    start = trimStart(buf, start, len);
    	    len = end - start;
    	    end = trimEnd(buf, start, len);
    	    len = end - start;
    	}
    
    	// Remove all parts of the string after / or &
    	for (int i = start; i < end; i++) {
    	    if (buf[i] == '/' || buf[i] == '&') {
    		end = i;
    		len = end - start;
    	    } else if (buf[i] == ' ') {
    		whitespaceCount++;
    		if (whitespaceCount >= 3) {
    		    end = i;
    		    len = end - start;
    		}
    	    } else {
    		switch(buf[i]) {
    		case ';':
    		case '(':
    		case ')':
    		case '#':
    		case '@':
    		    buf[i] = ' ';
    		}
    		    
    	    }
    	}
    
    	// This is the only suffix that causes trouble to the similarity
    	// function, because it confuses KING ST for KINGSTON, and similar cases
    	for (int i = start; i < end - 2; i++) {
    	    if (match(buf, i, 3, " ST")) {
    		// Delete only if suffix is last word or if there is a non-letter after
    		if (i + 3 == end || !Character.isUpperCase(buf[i + 3])) {
    		    end = i;
    		    len = end - start;
    		}
    	    }
    	}
    
    	start = trimStart(buf, start, len);
    	len = end - start;
    	end = trimEnd(buf, start, len);
    	len = end - start;
    	String s = new String(buf, start, len);
    	while(s.length() < 3) {
    	    s+= ' ';
    	}
    	return s;
    }

    private static int trimEnd(byte[] buf, int start, int len) {
        int end = start + len;
        boolean inWord = false;
        while (end > start && !inWord) {
            if (!Character.isUpperCase(buf[end - 1])) {
        	end--;
            } else {
        	inWord = true;
            }
        }
        return end;
    }

    private static int trimStart(byte[] buf, int start, int len) {
        boolean inWord = false;
        int i = start;
        while (!inWord && i < start + len) {
            if (!Character.isUpperCase(buf[i])) {
        	i++;
            } else {
        	inWord = true;
            }
        }
        return i;
    }
    
    /**
     * Decodes the specified portion of the buffer into a base-10 integer.
     * @param buf
     * @param offset
     * @param len
     * @return
     */
    public static int decodeInt(byte[] buf, int offset, int len) {
	int r = 0;
	for (int i = offset; i < offset + len; i++) {
	    r = r * 10;
	    r += Character.digit(buf[i], 10);
	}
	return r;
    }
}