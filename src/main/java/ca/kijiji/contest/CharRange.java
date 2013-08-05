package ca.kijiji.contest;

/**
 * A struct-like object to represent a range of indices within a char buffer
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
        this.buffer = str.toCharArray();
        this.end = this.buffer.length;
    }

    public void trim() {

        // Trim the head
        for(int i = this.start; i < this.end; ++i) {
            if(this.buffer[i] == ' ') {
                ++this.start;
            }
        }

        // Trim the tail
        for(int i = this.end - 1; i >= this.start; --i) {
            if(this.buffer[i] == ' ') {
                --this.end;
            }
        }
    }

    public char charAt(int i) {
        return buffer[start + i];
    }

    public int indexOf(char c) {
        for(int i = this.start; i < this.end; ++i) {
            if(buffer[i] == c) {
                return i;
            }
        }

        return -1;
    }

    /**
     * Apply this range to a character buffer and get the resulting string
     */
    public String slice() {
        return new String(buffer, start, end - start);
    }
}
