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

    public CharRange(char[] buffer, int start, int end) {
        this._start = start;
        this._end = end;
        this._buffer = buffer;
    }

    public CharRange(String str) {
        this._start = 0;
        this._buffer = str.toCharArray();
        this._end = this._buffer.length;
    }

    public CharRange() {
        this._start = 0;
        this._end = 0;
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

    public CharSequence subSequence(int start, int end) {
        assert(end >= 0 && end <= length());
        assert(start >= 0 && start < length());

        return new CharRange(_buffer, this._start + start, this._start + end);
    }

    public void substr(int start) {
        this._start += start;
    }

    /**
     * Split the CharRange into the specified list
     * @param list list to split into
     * @param sep character that separates entries
     * @param keepEmpty whether or not to keep empty entries
     */
    public void splitInto(List<CharRange> list, char sep, boolean keepEmpty) {
        //Based on splitWorker from Apache Commons

        int i;
        int start = i = this._start;

        boolean match = false;
        boolean lastMatch = false;
        while (i < this._end) {
            if (this._buffer[i] == sep) {
                if (match || keepEmpty) {
                    list.add(new CharRange(this._buffer, start, i));
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
            list.add(new CharRange(this._buffer, start, i));
        }
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
     * @return
     */
    public int toInteger() {
        int val = 0;
        for(int i = this._start; i < this._end; ++i) {
            int digit = Character.digit(_buffer[i], 10);

            if(digit == -1) {
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
     * Apply this range to a character buffer and get the resulting string
     */
    @Override
    public String toString() {
        return new String(_buffer, _start, _end - _start);
    }
}
