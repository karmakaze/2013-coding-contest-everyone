package ca.burnison.kijiji.contest;

import java.util.concurrent.ConcurrentMap;

import jsr166e.LongAdder;

/**
 * Encapsulates logic required to compute parking statistics.
 */
final class RevenueTask implements Runnable
{
    private final ConcurrentMap<String, LongAdder> tickets;
    private final TicketSource source;

    /**
     * @param tickets
     * @param source
     */
    RevenueTask(final ConcurrentMap<String, LongAdder> tickets, final TicketSource source)
    {
        this.tickets = tickets;
        this.source = source;
    }

    @Override
    public void run()
    {
        for(final Ticket ticket : this.source){
            if(ticket != null){
                this.compute(ticket);
            }
        }
    }

    private void compute(final Ticket ticket)
    {
        final String street = ticket.getStreet();
        if(street == null){
            return;
        }

        // Optimistically assume that the record exists. This branch should only happen on each new entry allowing for
        // branch predictions to provide a small speedup. Depending on the input source histogram, this may not
        // be the best approach. In such a case, consider using a plugable strategy.
        LongAdder revenue = tickets.get(street);
        if (revenue == null) {
            // The record didn't exist, but another thread may also be looking at it. Speculatively create a new adder
            // optimistically making it canonical. This comes at the cost of an allocation, but the total size is small.
            revenue = new LongAdder();

            // If our assumption doesn't hold true such that the write was actually contended and this thread lost, use
            // the winner. If this is run in a single thread, this will never happen.
            final LongAdder contendedRevenue = tickets.putIfAbsent(street, revenue);
            if (contendedRevenue != null) {
                revenue = contendedRevenue;
            }
        }
        revenue.add(ticket.getAmount());
    }
}
