package ca.kijiji.contest;

import java.io.*;

/**
 * Reads an InputStream piecemeal, putting it into a buffer that will fit the whole stream
 * returns ranges of indices in the buffer instead of String instances.
 *
 * This reduces the string processing work in producer threads in one producer many consumer
 * solutions.
 */
public class ChunkedBufferReader {

    // 73*900
    // Median line length is just over 73, this means we're not likely to spend much time looking
    // for the newline at the end of the chunk.
    private final int CHUNK_SIZE = 65700;

    // The buffer that the reader reads into
    private char[] _buffer;

    private final Reader _reader;
    private final InputStream _stream;

    // Our current position in the buffer
    private int _position = 0;

    public ChunkedBufferReader(InputStream stream) throws IOException {

        //we need a buffer as large as the input stream + CHUNK_SIZE or read() can fail
        _buffer = new char[stream.available() + CHUNK_SIZE + 1];

        _reader =  new InputStreamReader(stream);
        _stream = stream;
    }

    /**
     * @return the buffer backing the reader
     */
    public char[] getBuffer() {
        return _buffer;
    }


    /**
     * Read a chunk of lines into the buffer, the last character is guaranteed to be a newline or EOF.
     * @return The range that the read chunk occupies, or null if the stream is empty.
     * @throws IOException
      */
    public CharRange readChunkOfLines() throws IOException {

        if(_stream.available() <= 0)
            return null;

        // Read up to CHUNK_SIZE characters
        int start = _position;
        _position = start + _reader.read(_buffer, _position, CHUNK_SIZE);

        // Read up until the next newline or EOF
        _readLine();

        // Return the range of the chunk
        return new CharRange(start, _position);
    }

    /**
     * @return A single line from the stream, as a string.
     * @throws IOException
     */
    public String readLine() throws IOException {

        int start = _position;
        _readLine();

        return new CharRange(start, _position).slice(_buffer);
    }

    /**
     * Read in characters until the stream is empty or we hit a newline
     * @return the current last filled position in the buffer
     * @throws IOException
     */
    private int _readLine() throws IOException {

        while(_stream.available() > 0) {

            _position += _reader.read(_buffer, _position, 1);

            // This could break if a surrogate pair ends with a \n char!
            if(_buffer[_position - 1] == '\n') {
                break;
            }
        }

        return _position;
    }
}
