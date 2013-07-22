package ca.kijiji.contest;

import java.math.BigInteger;

/**
 */
public final class CRC24 implements java.util.zip.Checksum {

    final static int INIT = 0xb704ce;
    final static int POLY = 0x1864cfb;
    private int crc;

    /**
     * Creates a new CRC24 object.
     */
    public CRC24() {}

    /**
     * Creates a copy of a CRC24 object.
     * @param c object to be copied
     */
    public CRC24(CRC24 c) {
		crc = c.crc;
    }

    /**
     * Returns CRC-24 value.
     */
    public long getValue() {
		return crc & 0x00ffffff;
    }

    /**
     * Resets CRC-24 to initial value.
     */
    public void reset() {
		crc = INIT;
    }

    /**
     * Updates CRC-24 with specified byte.
     * @param b the byte to update the checksum with
     */
    public void update(int b) {
		int i = 8;
		crc ^= b << 16;
		while (i-- > 0) {
		    crc <<= 1;
		    if ((crc & 0x1000000) > 0)
		      crc ^= POLY;
		}
    }

    /**
     * Updates CRC-24 with specified array of bytes.
     * @param b the byte array to update the checksum with
     * @param off the start offset of the data
     * @param len the number of bytes to use for the update
     */
    public void update(byte[] b,int off,int len) {
		int i;
		int end = off + len;
		for (i=off; i < end; i++) update(b[i]);
    }

    /**
     * Updates CRC-24 with specified array of bytes.
     * @param b the byte array to update the checksum with
     */
    public void update(byte[] b) {
		update(b,0,b.length);
    }

    /**
     * Armor Checksum
     * @return Base64 representation prefixed by '='
     */
    public String toString() {
		byte[] c = {(byte)((crc >> 16) & 0x00ff), (byte)((crc >> 8) & 0x00ff), (byte)(crc & 0x00ff)};
		return toHex(c);
    }

    public static String toHex(byte[] bytes) {
        BigInteger bi = new BigInteger(1, bytes);
        return String.format("%0" + (bytes.length << 1) + "X", bi);
    }
}
