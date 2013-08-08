package ca.kijiji.contest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A runnable task that will read lines from an input stream and add those lines to a queue for processing
 * @author djmorton
 */
public class FileReaderTask implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(FileReaderTask.class);
    
    private static final int BUFFER_SIZE = 0x20000;
    
    private final Queue<String> queue;
    private final InputStream inputStream;    

    /**
     * Construct a FileReaderTask using the specified Queue and InputStream
     * @param queue A Queue on which read strings will be enqueued
     * @param inputStream The input stream from which strings will be read
     */
    public FileReaderTask(final Queue<String> queue, final InputStream inputStream) {
        this.queue = queue;
        this.inputStream = inputStream;
    }

    @Override
    public void run() {
        try (final BufferedReader reader = new BufferedReader(
                Channels.newReader(
                        Channels.newChannel(inputStream), "UTF-8"), 
                        BUFFER_SIZE)) {
            
            reader.readLine(); // Get rid of the title line
            
            String readLine = null;
            
            while ((readLine = reader.readLine()) != null) {
                queue.offer(readLine);
            }
            
        } catch (IOException e) {
            LOG.error("Error reading input file!", e);
        }
    }
}

