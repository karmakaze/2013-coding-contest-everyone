package com.lishid.kijiji.contest.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

/**
 * Converts bytes directly to chars so as to bypass character decoding that Java offers (which is quite slow)
 * 
 * @author lishid
 */
public class InputStreamAsciiReader extends Reader {
    InputStream input;
    byte[] buffer;
    
    public InputStreamAsciiReader(InputStream input) {
        this.input = input;
    }
    
    @Override
    public int read(char[] cbuf, int off, int len) throws IOException {
        if (buffer == null || buffer.length < len) {
            buffer = new byte[len];
        }
        
        int read = input.read(buffer, 0, len);
        
        for (int i = 0; i < len; i++) {
            cbuf[i + off] = (char) buffer[i];
        }
        
        return read;
    }
    
    @Override
    public void close() throws IOException {
        input.close();
    }
}
