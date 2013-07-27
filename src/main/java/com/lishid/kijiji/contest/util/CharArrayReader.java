package com.lishid.kijiji.contest.util;

/**
 * Used to read lines from a char array
 * 
 * @author lishid
 */
public class CharArrayReader {
    private char[] buffer;
    private int offset = 0;
    private int end;
    
    public CharArrayReader(char[] buffer, int offset, int length) {
        this.buffer = buffer;
        this.offset = offset;
        this.end = offset + length;
    }
    
    /**
     * Reads a line of text. A line is considered to be terminated by any one
     * of a line feed ('\n'), a carriage return ('\r'), or a carriage return
     * followed immediately by a line feed.
     * 
     * @return A String containing the contents of the line, not including
     *         any line-termination characters, or null if the end of the
     *         char array has been reached
     */
    public PseudoString readLine() {
        if (offset >= end) {
            return null;
        }
        
        int startIndex = offset;
        int endIndex;
        boolean foundCR = false;
        for (endIndex = startIndex; endIndex < end; endIndex++) {
            char c = buffer[endIndex];
            if (c == '\r') {
                foundCR = true;
            }
            else if (c == '\n') {
                offset = endIndex + 1;
                break;
            }
            else if (foundCR) {
                offset = endIndex;
                break;
            }
        }
        if (endIndex == end) {
            offset = end;
        }
        if (foundCR) {
            endIndex--;
        }
        
        if (endIndex == startIndex) {
            return readLine();
        }
        
        return new PseudoString(buffer, startIndex, endIndex - startIndex);
    }
    
    /**
     * Gets the underlying buffer
     * 
     * @return the underlying buffer
     */
    public char[] getBuffer() {
        return buffer;
    }
}
