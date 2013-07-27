package com.lishid.kijiji.contest.util;

import java.util.ArrayList;
import java.util.List;

/**
 * PseudoString is like a Java String, but it re-uses the char array given
 * 
 * @author lishid
 */
public class PseudoString implements Comparable<PseudoString> {
    public char[] data;
    public int start;
    public int length;
    public int end;
    private int hash;
    
    public PseudoString(String string) {
        this(string.toCharArray(), 0, string.length());
    }
    
    public PseudoString(char[] data, int start, int length) {
        this.data = data;
        this.start = start;
        this.length = length;
        this.end = start + length;
    }
    
    @Override
    public String toString() {
        return new String(data, start, length);
    }
    
    @Override
    public int hashCode() {
        int h = hash;
        if (h == 0 && length > 0) {
            for (int i = start; i < end; i++) {
                h = 31 * h + data[i];
            }
            hash = h;
        }
        return h;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof PseudoString)) {
            return false;
        }
        PseudoString another = (PseudoString) obj;
        if (another.length != this.length) {
            return false;
        }
        // Chars comparison
        for (int i = this.start, j = another.start; i < length; i++, j++) {
            if (this.data[i] != another.data[j]) {
                return false;
            }
        }
        return true;
    }
    
    @Override
    public int compareTo(PseudoString another) {
        int len1 = data.length;
        int len2 = another.data.length;
        int lim = Math.min(len1, len2);
        char v1[] = data;
        char v2[] = another.data;
        
        int start1 = start;
        int start2 = another.start;
        int end = start1 + lim;
        while (start1 < end) {
            char c1 = v1[start1];
            char c2 = v2[start2];
            if (c1 != c2) {
                return c1 - c2;
            }
            start1++;
            start2++;
        }
        
        return len1 - len2;
    }
    
    public boolean isEmpty() {
        return length == 0;
    }
    
    public PseudoString[] split(char c) {
        List<PseudoString> result = new ArrayList<PseudoString>();
        int last = start;
        for (int i = start; i < end; i++) {
            if (data[i] == c) {
                result.add(new PseudoString(data, last, i - last));
                last = i + 1;
            }
        }
        if (last < end) {
            result.add(new PseudoString(data, last, end - last));
        }
        if (result.isEmpty()) {
            return new PseudoString[] { this };
        }
        return result.toArray(new PseudoString[result.size()]);
    }
    
    /**
     * Special split where only selected columns are returned, thus saving memory and cpu
     * 
     * @param c character to split by
     * @param columns integer array containing the columns (0 indexed), ending with an extra element (-1)
     * @return
     */
    public PseudoString[] split(char c, int[] columns) {
        List<PseudoString> result = new ArrayList<PseudoString>(columns.length - 1);
        int last = start;
        int nextColumnCheck = 0;
        int column = 0;
        for (int i = start; i < end; i++) {
            if (data[i] == c) {
                if (columns[nextColumnCheck] == column) {
                    nextColumnCheck += 1;
                    result.add(new PseudoString(data, last, i - last));
                }
                last = i + 1;
                column += 1;
            }
        }
        if (last < end && columns[nextColumnCheck] == column) {
            result.add(new PseudoString(data, last, end - last));
        }
        if (result.isEmpty()) {
            return new PseudoString[] { this };
        }
        return result.toArray(new PseudoString[result.size()]);
    }
    
    public boolean isAlphabetic() {
        if (isEmpty())
            return false;
        
        for (int i = start; i < end; i++) {
            if (!Character.isLetter(data[i]) && data[i] != '\'') {
                return false;
            }
        }
        
        return true;
    }
    
    public int toPositiveInteger() {
        int result = 0;
        for (int i = start; i < end; i++) {
            char c = data[i];
            if (c >= '0' && c <= '9') {
                result = result * 10 + c - '0';
            }
            else if (c != ' ') {
                return 0;
            }
        }
        return result;
    }
}
