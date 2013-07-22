package ca.kijiji.contest.ticketworkers;

import ca.kijiji.contest.CSVUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

public abstract class AbstractTicketWorker extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(StreetFineTabulator.class);

    protected static final int NUM_CSV_COLS = 11;

    protected static final int ADDR_COLUMN = 7;
    protected static final int FINE_COLUMN = 4;

    // Message that marks the end of processing.
    public static final String END_MSG = "\n\n\n";

    // decrement this when we leave run(), means no running worker threads when at 0
    protected final CountDownLatch _mRunningCounter;

    // How the main thread communicates with us
    protected final LinkedBlockingQueue<String> _mMessageQueue;



    protected AbstractTicketWorker(CountDownLatch counter, LinkedBlockingQueue<String> queue) {
        _mRunningCounter = counter;
        _mMessageQueue = queue;
    }


    public void run () {

        // Start the infinite message loop, quit when we get an END message.
        for(;;) {
            try {

                // Block until we have a new message
                String message;
                while((message =  _mMessageQueue.poll()) == null) {

                }

                // It's the last message, rebroadcast to all the other consumers so they shut down as well.
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
                if(ticketCols.length != NUM_CSV_COLS) {

                    // Process the CSV line *properly*
                    ticketCols = CSVUtils.parseCSVLine(message);

                    // Do we have the correct number of columns now?
                    if(ticketCols.length != NUM_CSV_COLS) {

                        // Print an error and skip to the next line
                        String msg = String.format("Expected %d columns, got %d (invalid tickets file?):\n%s",
                                NUM_CSV_COLS, ticketCols.length, message);
                        LOG.warn(msg);
                        continue;
                    }
                }

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
