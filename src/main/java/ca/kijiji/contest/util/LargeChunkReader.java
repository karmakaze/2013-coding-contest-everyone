package ca.kijiji.contest.util;

import java.io.IOException;
import java.io.Reader;

/**
 * Used to read large chunks from a Reader and splitting the output at line terminators.
 * 
 * @author lishid
 */
public class LargeChunkReader {
    private Reader input;
    /** Used to store the beginning of the last cut-off chunk of chars */
    private char[] chunkBuffer;
    private int chunkBufferSize;
    
    public LargeChunkReader(Reader input) {
        this.input = input;
    }
    
    /**
     * Reads the input by chunk of determined size and partition (cut off) at line terminators.
     * 
     * @param buffer
     *            The output buffer of the data
     * @return The number of characters read, or -1 if the end of the stream has been reached
     * @throws IOException
     */
    public int readChunk(char[] buffer) throws IOException {
        int bufferIndex = 0;
        
        // Copy previous chunkBuffer to buffer
        if (chunkBuffer != null && chunkBufferSize > 0) {
            System.arraycopy(chunkBuffer, 0, buffer, 0, chunkBufferSize);
            bufferIndex = chunkBufferSize;
        }
        // Clean up chunkBuffer
        chunkBufferSize = 0;
        
        // Read from reader to buffer
        int read = readUntilFull(buffer, bufferIndex);
        
        // Input is done reading
        if (read < 0) {
            // Read out from chunkBuffer first
            if (bufferIndex > 0) {
                return bufferIndex;
            }
            // Nothing to be read anymore
            else {
                return -1;
            }
        }
        
        bufferIndex += read;
        
        int newLineIndex = 0;
        int newLineChars = 0;
        
        // Look backwards for newlines
        for (newLineIndex = bufferIndex - 1; newLineIndex >= 0; newLineIndex--) {
            if (buffer[newLineIndex] == '\r') {
                newLineChars += 1;
            }
            else if (buffer[newLineIndex] == '\n') {
                newLineChars += 1;
            }
            else if (newLineChars > 0) {
                newLineIndex += 1;
                break;
            }
        }
        
        // No newlines! This is not supposed to happen if the size of buffer is larger than the longest of lines
        if (newLineIndex < 0) {
            // TODO Throw exception?
            System.out.println("Newline not found!! Chunk size " + buffer.length + " not large enough.");
            return bufferIndex;
        }
        
        // Calculate how much needs to be moved to chunkBuffer
        int nextPrevBufferSize = bufferIndex - newLineIndex - newLineChars;
        
        // Check if size is large enough
        if (chunkBuffer == null || chunkBuffer.length < nextPrevBufferSize) {
            chunkBuffer = new char[Math.max(buffer.length, nextPrevBufferSize)];
        }
        
        // Move extra to prevBuffer
        System.arraycopy(buffer, newLineIndex + newLineChars, chunkBuffer, 0, nextPrevBufferSize);
        chunkBufferSize = nextPrevBufferSize;
        
        return newLineIndex;
    }
    
    private int readUntilFull(char[] buffer, int startIndex) throws IOException {
        int read = 0;
        // Keep reading until we filled the buffer
        while (startIndex < buffer.length) {
            int numRead = input.read(buffer, startIndex, buffer.length - startIndex);
            if (numRead < 0) {
                if (read == 0) {
                    return -1;
                }
                break;
            }
            read += numRead;
            startIndex += numRead;
        }
        return read;
    }
    
    public void close() throws IOException {
        input.close();
    }
}
