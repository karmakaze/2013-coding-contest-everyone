package ca.kijiji.contest;

/**
 * A struct-like object to represent a range of indices within a string
 */
public class CharRange {
    public final int start;
    public final int end;

    public CharRange(int start, int end) {
        this.start = start;
        this.end = end;
    }

    /**
     * Apply this range to a character buffer and get the resulting string
     */
    public String slice(char[] array) {
        return new String(array, start, end - start);
    }
}
