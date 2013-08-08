package ca.kijiji.contest;

import java.util.LinkedList;
import java.util.List;

/**
 * Represents a mutable view of a character buffer, supports string-like operations
 */
public class CharRange implements CharSequence {
    private char[] _buffer;
    private int _start;
    private int _end;

    static final char[] EMPTY_BUFFER = new char[]{};

    /**
     * Create a CharRange backed by an external char array
     */
    public CharRange(char[] buffer, int start, int end) {
        set(buffer, start, end);
    }

    /**
     * Create a CharRange backed by a string's char array
     */
    public CharRange(String str) {
        char[] strBuf = str.toCharArray();
        set(strBuf, 0, strBuf.length);
    }

    /**
     * An empty CharRange
     */
    public CharRange() {
        set(EMPTY_BUFFER, 0, 0);
    }

    /**
     * Sets the contents of a CharRange, useful for avoiding object creation penalties when you can
     * re-use CharRange instances.
     */
    public void set(char[] buffer, int start, int end) {
        this._buffer = buffer;
        this._start = start;
        this._end = end;
    }

    public int length() {
        return this._end - this._start;
    }

    public boolean isEmpty() {
        return length() == 0;
    }

    /**
     * Get the character at the specified index
     */
    public char charAt(int i) {
        return this._buffer[this._start + i];
    }

    /**
     * Get the index of the first instance of a character, or -1 if none found.
     */
    public int indexOf(char c) {
        for(int i = this._start; i < this._end; ++i) {
            if(this._buffer[i] == c) {
                return i - this._start;
            }
        }

        return -1;
    }

    /**
     * Get a slice of the CharRange starting from start and ending at end
     * @param start start relative to instance's this._start
     * @param end end relative to instance's this._start
     */
    public CharSequence subSequence(int start, int end) {
        assert(end >= 0 && end <= length());
        assert(start >= 0 && start < length());

        return new CharRange(this._buffer, this._start + start, this._start + end);
    }

    /**
     * Split the CharRange into the specified list
     * If the List contains any CharRanges they will be re-used instead of creating new CharRange instances.
     * @param list list to split into
     * @param sep character that separates entries
     * @param keepEmpty whether or not to keep empty entries
     * @return number of matches found
     */
    public int splitInto(List<CharRange> list, char sep, boolean keepEmpty) {
        //Based on splitWorker from Apache Commons

        int i;
        int numMatches = 0;
        int start = i = this._start;

        boolean match = false;
        boolean lastMatch = false;
        while (i < this._end) {
            if (this._buffer[i] == sep) {
                if (match || keepEmpty) {

                    if(numMatches >= list.size())
                        list.add(new CharRange());

                    list.get(numMatches).set(this._buffer, start, i);

                    ++numMatches;

                    match = false;
                    lastMatch = true;
                }
                start = ++i;
                continue;
            }
            lastMatch = false;
            match = true;
            i++;
        }

        if (match || keepEmpty && lastMatch) {
            if(numMatches >= list.size())
                list.add(new CharRange());

            list.get(numMatches).set(this._buffer, start, i);

            ++numMatches;
        }

        // we can't use list size to detemine how many matches there were, return numMatches
        return numMatches;
    }

    /**
     * Split the CharRange into a list of entries
     * @param sep character that separates entries
     * @param keepEmpty whether or not to keep empty entries
     */
    public List<CharRange> split(char sep, boolean keepEmpty) {
        List<CharRange> list = new LinkedList<>();
        splitInto(list, sep, keepEmpty);
        return list;
    }

    /**
     * Parses a non-negative integer out of the start of a CharRange
     * Assumes that the CharRange has been trimmed and cleaned beforehand
     * @return the parsed number, or null if invalid
     */
    public Integer toInteger() {
        Integer val = 0;
        for(int i = this._start; i < this._end; ++i) {
            int digit = Character.digit(_buffer[i], 10);

            if(digit == -1) {
                // No valid numbers in this string? return null.
                if(i == this._start) {
                    val = null;
                }
                break;
            }

            val = (val * 10) + digit;
        }

        return val;
    }

    /**
     * Remove spaces from the beginning and end of the CharSequence
     */
    public void trim() {

        // Trim the head
        int i;
        for(i = this._start; i < this._end; ++i) {
            if(this._buffer[i] != ' ') {
                break;
            }
        }

        this._start = i;

        // Trim the tail
        for(i = this._end - 1; i >= this._start; --i) {
            if(this._buffer[i] != ' ') {
                break;
            }
        }

        this._end = i + 1;
    }

    /**
     * Return a slice of the CharRange as a string, beginning at start
     * @param start where to begin the slice (inclusive)
     */
    public String strSlice(int start) {
        return new String(this._buffer, start + this._start, this._end - this._start - start);
    }

    /**
     * Apply this range to a character buffer and get the resulting string
     */
    @Override
    public String toString() {
        return new String(this._buffer, this._start, this._end - this._start);
    }
}
