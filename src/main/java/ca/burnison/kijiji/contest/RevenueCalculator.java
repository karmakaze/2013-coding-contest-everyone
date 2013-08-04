package ca.burnison.kijiji.contest;

import java.util.Comparator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import jsr166e.LongAdder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ComparisonChain;

/**
 * A multi-threaded calculator that generates a sorted (revenue descending) map of parking ticket revenue by street.
 * This implementation uses an in-memory store to retain statistics. Obviously, this technique will only work when there
 * is a finite number of unique keys. In the case of an arbitrarily large number of keys, a more efficient storage
 * mechanism (likely diskbound) should be considered.
 */
@ThreadSafe
public final class RevenueCalculator
{
    private static final Logger logger = LoggerFactory.getLogger(RevenueCalculator.class);

    /** The initial size of the backing store. This value was chosen empirically based on the files at hand. */
    private static final int INITIAL_SIZE = 50_000;

    /**
     * A sync point for analysis, keyed by street mapping to a summing mechanism. Because this collection is almost
     * entirely write-only, the use of an AtomicLong may result in unnecessary CAS loops on machines with many cores. An
     * adder helps solve this problem at the cost of (theoretically) slower reads. Since reads only happen once, and in
     * an uncontended way, after the map is fully generated, this makes the most sense.
     */
    private final ConcurrentMap<String, LongAdder> tickets;
    private final ExecutorService es;

    @GuardedBy("this")
    private volatile SortedMap<String, Integer> calculated = null;

    /**
     * Create a new revenue calculator.
     * @param consumers The number of threads that will calculate the revenue.
     */
    public RevenueCalculator(final int consumers)
    {
        // The maximum capacity of the work queue is likely an empirical number and probably platform specific. However,
        // it seems that an CONSUMERS order of two scales well enough. It's important that the queue is large enough so
        // that the writer thread is constantly busy, but small enough that when the queue fills up, the writer can
        // steal some work from the back of the queue without the queue draining.
        final int size = consumers * (1 << consumers);
        final BlockingQueue<Runnable> q = new ArrayBlockingQueue<>(size);

        // Because accuracy is fairly important, a drop-on-reject policy won't work. The best way to handle a rejection
        // is by work stealing. If the running queues start to backup, the the writer can let the readers catch up. 
        final RejectedExecutionHandler h = new ThreadPoolExecutor.CallerRunsPolicy();

        // When work stealing, the concurrency level has to be one more than the specified number of consumers
        this.tickets = new ConcurrentHashMap<>(INITIAL_SIZE, 0.75f, consumers + 1);
        this.es = new ThreadPoolExecutor(consumers, consumers, 1000, TimeUnit.MILLISECONDS, q, h);
    }

    /**
     * Add the specified tickets into the calculator. The number of tickets in the specified source should be dense
     * enough that meaningful work can be accomplished across multiple cores. If the number of tickets is too small, too
     * much CPU time will be spent performing CAS swaps.
     * @param source
     * @throws IllegalStateException If no longer accepting data.
     */
    public void add(final TicketSource source)
    {
        if(this.calculated != null){
            throw new IllegalStateException("Calculation is already complete.");
        }

        try{
            this.es.submit(new RevenueTask(this.tickets, source));
        } catch(final RejectedExecutionException ex){
            throw new IllegalStateException("No longer accepting data.");
        }
    }

    /**
     * Calculates the revenue for all added tickets and returns. This method may block depending on the number of
     * tickets added prior to calling this method. Once this method has been called, the calculator will no longer
     * accept new tickets.
     * @param timeoutMinutes The timeout, in minutes, this task has to complete.
     * @return A map sorted by revenue in descending order and keyed by a normalized street name.
     */
    public synchronized SortedMap<String, Integer> calculate(final int timeoutMinutes)
    {
        if(this.calculated != null){
            return this.calculated;
        }

        this.shutdown(timeoutMinutes);
        logger.info("Finished calculation with {} results.", this.tickets.size());

        final SortedMap<String, Integer> sorted = new TreeMap<>(new RevenueMapComparator(tickets));
        for (final Map.Entry<String, LongAdder> e : tickets.entrySet()) {
            sorted.put(e.getKey(), e.getValue().intValue());
        }

        this.calculated = sorted;
        return sorted;
    }

    private void shutdown(final int timeoutMinutes)
    {
        try {
            this.es.shutdown();
            if (!this.es.awaitTermination(timeoutMinutes, TimeUnit.MINUTES)) {
                this.es.shutdownNow();
            }
        } catch (final InterruptedException ex) {
            this.es.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * A very lame hack that simply delegates sorting to the original map. Because the comparator is used during gets,
     * <code>o1</code> can be an unknown key. In this case, the query to the delegate will return null. In this case,
     * this implementation lies and pretend the values are different.
     */
    private static final class RevenueMapComparator implements Comparator<String>
    {
        private final Map<String, LongAdder> tickets;

        public RevenueMapComparator(final Map<String, LongAdder> tickets)
        {
            this.tickets = tickets;
        }

        @Override
        public int compare(final String o1, final String o2)
        {
            final LongAdder revenue1 = tickets.get(o1);
            if (revenue1 == null) {
                return 1;
            }
            final LongAdder revenue2 = tickets.get(o2);
            return ComparisonChain.start()
                .compare(revenue2.intValue(), revenue1.intValue())
                .compare(o2, o1)
                .result();
        }
    }
}
