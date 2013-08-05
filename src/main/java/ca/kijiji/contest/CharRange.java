package ca.kijiji.contest;

import java.util.LinkedList;
import java.util.List;

/**
 * Represents a mutable view of a character buffer, supports string-like operations
 */
public class CharRange {
    public char[] buffer;
    public int start;
    public int end;

    public CharRange(char[] buffer, int start, int end) {
        this.start = start;
        this.end = end;
        this.buffer = buffer;
    }

    public CharRange(String str) {
        this.start = 0;

        //bad hacky workaround to remove later.
        if(str != null) {
            this.buffer = str.toCharArray();
            this.end = this.buffer.length;
        } else {
            this.buffer = null;
            this.end = 0;
        }
    }

    public CharRange() {}

    public void trim() {

        // Trim the head
        int i;
        for(i = this.start; i < this.end; ++i) {
            if(this.buffer[i] != ' ') {
                break;
            }
        }

        this.start = i;

        // Trim the tail
        for(i = this.end - 1; i >= this.start; --i) {
            if(this.buffer[i] != ' ') {
                break;
            }
        }

        this.end = i + 1;
    }

    public char charAt(int i) {
        return buffer[start + i];
    }

    public int indexOf(char c) {
        for(int i = this.start; i < this.end; ++i) {
            if(buffer[i] == c) {
                return i - this.start;
            }
        }

        return -1;
    }

    public boolean isEmpty() {
        return this.end <= this.start;
    }

    public void splitInto(List<CharRange> list, char sep, boolean preserveNull) {

        int i;
        int start = i = this.start;

        boolean match = false;
        boolean lastMatch = false;
        while (i < this.end) {
            if (buffer[i] == sep) {
                if (match || preserveNull) {
                    list.add(new CharRange(buffer, start, i));
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

        if (match || preserveNull && lastMatch) {
            list.add(new CharRange(buffer, start, i));
        }
    }

    public List<CharRange> split(char sep, boolean preserveNull) {
        List<CharRange> list = new LinkedList<>();
        splitInto(list, sep, preserveNull);
        return list;
    }

    /**
     * Parses a non-negative integer out of the start of a CharRange
     * Assumes that the CharRange has been trimmed and cleaned beforehand
     * @return
     */
    public int toInteger() {
        int val = 0;
        for(int i = this.start; i < this.end; ++i) {
            int digit = Character.digit(buffer[i], 10);

            if(digit == -1) {
                break;
            }

            val = (val * 10) + digit;
        }

        return val;
    }

    /**
     * Apply this range to a character buffer and get the resulting string
     */
    @Override
    public String toString() {
        return new String(buffer, start, end - start);
    }
}
