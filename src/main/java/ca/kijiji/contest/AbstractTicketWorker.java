package ca.kijiji.contest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;
import org.slf4j.*;

/**
 * Base class for Asynchronous ticket processors
 */
abstract class AbstractTicketWorker extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(StreetProfitTabulator.class);

    private int _mNumCSVCols;

    private List<String> _mCSVCols = new ArrayList<>();

    // Message that marks the end of processing.
    public static final CharRange END_MSG = new CharRange(-1, -1);

    // decrement this when we leave run(), means no running worker threads when at 0
    private final CountDownLatch _mRunningCounter;

    // How the main thread communicates with us
    private final LinkedBlockingQueue<CharRange> _mMessageQueue;

    // Number of errors we've come across during our work
    protected final AtomicInteger mErrCounter;

    // Buffer to read chunks from
    protected char[] mBuffer;


    protected AbstractTicketWorker(CountDownLatch runCounter, AtomicInteger errCounter, LinkedBlockingQueue<CharRange> queue,
                                   char[] buffer) {
        _mRunningCounter = runCounter;
        _mMessageQueue = queue;
        mBuffer = buffer;
        mErrCounter = errCounter;
    }

    /**
     * Set the column indexes for the relevant fields given a parsed CSV header
     */
    public void setColumns(String[] cols) {
        _mNumCSVCols = cols.length;
        _mCSVCols = Arrays.asList(cols);
    }

    /**
     * Given an array representing the columns of a row, return the column associated with colName
     */
    protected String getColumn(String[] cols, String colName) {
        return cols[_mCSVCols.indexOf(colName)];
    }

    public void run () {

        // Make sure we've called setColumns
        assert(_mNumCSVCols != 0);

        // Start the infinite message loop, quit when we get an END message.
        for(;;) {
            try {

                // Block until we have a new message.
                CharRange message;
                while((message =  _mMessageQueue.poll()) == null) {
                    Thread.yield();
                }

                // If it's the last message, rebroadcast to all the other consumers so they shut down as well.
                if(message == END_MSG) {
                    _mMessageQueue.put(END_MSG);
                    break;
                }


                // Process the chunk the producer gave us into separate rows
                String strMessage = message.slice(mBuffer);
                for(String ticketRow : StringUtils.split(strMessage, '\n')) {

                    // Split the ticket into columns, this isn't CSV compliant and will
                    // fail on columns with escaped values. There's less than 100 of those
                    // in the test data, so do it the quick way unless something goes wrong.
                    String[] ticketCols = StringUtils.splitPreserveAllTokens(ticketRow, ',');

                    // Is this line properly formed? (This check will fail on valid CSVs
                    // with variable column numbers and embedded commas)
                    if(ticketCols.length != _mNumCSVCols) {

                        // Process the CSV line *properly*
                        ticketCols = CSVUtils.parseCSVLine(ticketRow);

                        // Do we have the correct number of columns now?
                        if(ticketCols.length != _mNumCSVCols) {

                            // Guess not, print an error and skip to the next line
                            String msg = String.format("Expected %d columns, got %d (invalid tickets file?):\n%s",
                                    _mNumCSVCols, ticketCols.length, ticketRow);
                            LOG.warn(msg);
                            continue;
                        }
                    }

                    // Implementation-defined method of processing the columns
                    processTicketCols(ticketCols);
                }

            } catch (InterruptedException e) {
                return;
            }
        }
        _mRunningCounter.countDown();
    }

    /**
     * Do something with the split up columns from a line in the CSV
     * @param ticketCols columns from the CSV
     */
    abstract protected void processTicketCols(String[] ticketCols);
}
