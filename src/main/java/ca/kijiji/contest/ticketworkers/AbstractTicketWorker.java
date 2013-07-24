package ca.kijiji.contest.ticketworkers;

import ca.kijiji.contest.CSVUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.*;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractTicketWorker extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(StreetProfitTabulator.class);

    private static int mNumCSVCols;

    protected static int mAddressColIdx;
    protected static int mFineColIdx;

    // Message that marks the end of processing. Use something that won't show up in any other valid
    // message in case String.intern is called on message sent (we use identity comparison.)
    public static final String END_MSG = "\n\n\n";

    // decrement this when we leave run(), means no running worker threads when at 0
    private final CountDownLatch _mRunningCounter;

    // How the main thread communicates with us
    private final LinkedBlockingQueue<String> _mMessageQueue;

    // Number of errors we've come across during our work
    protected final AtomicInteger mErrCounter;



    protected AbstractTicketWorker(CountDownLatch runCounter, AtomicInteger errCounter, LinkedBlockingQueue<String> queue) {
        _mRunningCounter = runCounter;
        _mMessageQueue = queue;

        mErrCounter = errCounter;
    }

    /**
     * Set the column indexes for the relevant fields given a parsed CSV header
     * @param cols CSV header columns
     */
    public void setColumns(String[] cols) {
        mNumCSVCols = cols.length;

        List<String> colsList = Arrays.asList(cols);
        mAddressColIdx = colsList.indexOf("location2");
        mFineColIdx = colsList.indexOf("set_fine_amount");
    }

    public void run () {

        // Make sure we've called setColumns
        assert(mNumCSVCols != 0);

        // Start the infinite message loop, quit when we get an END message.
        for(;;) {
            try {

                // Block until we have a new message.
                String message;
                while((message =  _mMessageQueue.poll()) == null) {
                    Thread.yield();
                }

                // If it's the last message, rebroadcast to all the other consumers so they shut down as well.
                if(message == END_MSG) {
                    _mMessageQueue.put(END_MSG);
                    break;
                }

                // Split the ticket into columns, this isn't CSV compliant and will
                // fail on columns with escaped values. There's less than 100 of those
                // in the test data, so do it the quick way unless something goes wrong.
                String[] ticketCols = StringUtils.splitPreserveAllTokens(message, ',');

                // Is this line properly formed? (This check will fail on valid CSVs
                // with variable column numbers and embedded commas)
                if(ticketCols.length != mNumCSVCols) {

                    // Process the CSV line *properly*
                    ticketCols = CSVUtils.parseCSVLine(message);

                    // Do we have the correct number of columns now?
                    if(ticketCols.length != mNumCSVCols) {

                        // Print an error and skip to the next line
                        String msg = String.format("Expected %d columns, got %d (invalid tickets file?):\n%s",
                                mNumCSVCols, ticketCols.length, message);
                        LOG.warn(msg);
                        continue;
                    }
                }

                // Implementation-defined method of processing the columns
                processTicketCols(ticketCols);
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
