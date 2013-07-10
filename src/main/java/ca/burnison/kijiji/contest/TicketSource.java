package ca.burnison.kijiji.contest;

/**
 * To keep calculations implementation agnostic, calculators are fed by a ticket source. The main reason for using such
 * a source is the inherent cost of parsing a serialized blob. Each implementation should be a specialization for a
 * given input type. Implementations are free to return nulls as valid values within the iterator.
 * <p>
 * Implementations need not be thread safe.
 */
public interface TicketSource extends Iterable<Ticket>
{
}
