package ca.kijiji.contest;

import java.util.Iterator;

import ca.burnison.kijiji.contest.AddressParser;
import ca.burnison.kijiji.contest.Ticket;
import ca.burnison.kijiji.contest.TicketSource;

/**
 * A ticket source that is sourced by a CSV file issued by the Toronto parking authority.
 */
final class TorontoCsvTicketSource implements TicketSource
{
    private static final int CSV_COLUMNS = 11;
    private static final int CSV_INDEX_AMOUNT = 4;
    private static final int CSV_INDEX_LOCATION_2 = 7;

    private final String[] csv;
    private final AddressParser parser;
    
    /**
     * 
     * @param csv
     * @param parser
     */
    TorontoCsvTicketSource(final String[] csv, final AddressParser parser)
    {
        this.csv = csv;
        this.parser = parser;
    }

    @Override
    public Iterator<Ticket> iterator()
    {
        return new Iterator<Ticket>()
        {
            private int index = 0;
            
            @Override
            public boolean hasNext()
            {
                return this.index < csv.length;
            }

            @Override
            public Ticket next()
            {
                return parse(csv[this.index++]);
            }

            @Override
            public void remove()
            {
                throw new UnsupportedOperationException("Iterator does not support removal.");
            }
        };
    }
    
    /**
     * Create a new instance from the specified line of CSV.
     * @param line
     * @return A new ticket, or null if unable to parse the line.
     * @throws NumberFormatException If the amount cannot be parsed.
     */
    private Ticket parse(final String line)
    {
        if(line == null){
            return null;
        }
        
        // While string.split() would make the most sense, it was a massive hotspot. The following loop is a lazy
        // implementation that drops anything that isn't important. Should the usecase change to require more than
        // two columns, it may make sense to revert back to using split().
        int found = 0;
        final StringBuilder street = new StringBuilder(25);
        final StringBuilder amount = new StringBuilder(3);
        for(final char c : line.toCharArray()){
            if(found > CSV_INDEX_LOCATION_2) break;
            else if(c == ',') found++;
            else if(found == CSV_INDEX_AMOUNT) amount.append(c);
            else if(found == CSV_INDEX_LOCATION_2) street.append(c);
        }
        if(found < CSV_INDEX_LOCATION_2){
            return null;
        }

        // Assume that the value is going to be parsable. This may not hold true across all data types, but it's a
        // reasonable assumption for the current data sets. The best thing to do in this case is fail loudly. Let the
        // format exception bubble up.
        final int parsedAmount = Integer.parseInt(amount.toString());
        final String parsedStreet = this.parser.parse(street.toString());
        return new Ticket(parsedStreet, parsedAmount);
    }
}
