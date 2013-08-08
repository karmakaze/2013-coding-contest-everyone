package ca.kijiji.contest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.*;

/**
 * Base class for Asynchronous ticket processors
 */
abstract class AbstractTicketWorker extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(StreetProfitTabulator.class);

    private int _mNumCSVCols;

    private List<String> _mCSVCols = new ArrayList<>();

    // Message that marks the end of processing.
    public static final CharRange END_MSG = new CharRange();

    // decrement this when we leave run(), means no running worker threads when at 0
    private final CountDownLatch _mRunningCounter;

    // How the main thread communicates with us
    private final LinkedBlockingQueue<CharRange> _mMessageQueue;

    // Number of errors we've come across during our work
    protected final AtomicInteger mErrCounter;

    protected AbstractTicketWorker(CountDownLatch runCounter, AtomicInteger errCounter, LinkedBlockingQueue<CharRange> queue) {
        _mRunningCounter = runCounter;
        _mMessageQueue = queue;
        mErrCounter = errCounter;
    }

    /**
     * Set the column indexes for the relevant fields given a parsed CSV header
     */
    abstract protected void implSetColumns(List<String> cols);

    public void setColumns(String[] cols) {
        _mNumCSVCols = cols.length;
        _mCSVCols = Arrays.asList(cols);

        // Let the implementation cache any indexes it cares about
        implSetColumns(_mCSVCols);
    }

    public void run () {

        // Make sure we've called setColumns
        assert(_mNumCSVCols != 0);

        // Keep these lists around so we can re-use CharRange instances when we use splitInto()
        List<CharRange> ticketCols = new ArrayList<>(0);
        List<CharRange> ticketRows = new ArrayList<>(0);

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
                int numRows = message.splitInto(ticketRows, '\n', false);

                for(int i = 0; i < numRows; ++i) {
                    CharRange ticketRow = ticketRows.get(i);

                    // Split the ticket into columns, this isn't CSV compliant and will
                    // fail on columns with escaped values. There's less than 100 of those
                    // in the test data, so do it the quick way unless something goes wrong.
                    int numCols = ticketRow.splitInto(ticketCols, ',', true);

                    // Is this line properly formed? (This check will fail on valid CSVs
                    // with variable column numbers and embedded commas)
                    if(numCols != _mNumCSVCols) {

                        // List length now has to be meaningful, get rid of cached CharRange instances
                        ticketCols.clear();

                        // Process the CSV line *properly*
                        for(String col : CSVUtils.parseCSVLine(ticketRow.toString())) {
                            ticketCols.add(new CharRange(col));
                        }

                        // Do we have the correct number of columns now?
                        if(ticketCols.size() != _mNumCSVCols) {

                            // Guess not, print an error and skip to the next line
                            String msg = String.format("Expected %d columns, got %d (invalid tickets file?):\n%s",
                                    _mNumCSVCols, ticketCols.size(), ticketRow);
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
    abstract protected void processTicketCols(List<CharRange> ticketCols);
}
