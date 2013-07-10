package ca.kijiji.contest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

import java.util.SortedMap;

import ca.burnison.kijiji.contest.Latin1AddressParser;
import ca.burnison.kijiji.contest.RevenueCalculator;
import ca.burnison.kijiji.contest.TicketSource;

/**
 * This implementation reads from the input stream with a single thread and then performs the work across a specified
 * number of threads (a "calculator"). This is similar to a fork/join implementation but works on an unbounded stream
 * of data.
 * <p>
 * The number of workers in the calculator should be be less than the actual number of available processors on the
 * system, otherwise, the writer will become contended and the system will livelock. Ideally, the number of consumers 
 * should be at least 1 less than the number of avaialable cores, however, this is not a requirement.
 */
public class ParkingTicketsStats {
    private static final int CONSUMERS = Math.max(1, Runtime.getRuntime().availableProcessors() - 1);
    private static final int TIMEOUT_MINUTES = 30;

    /**
     * To reduce the amount of overhead, give each task a meaningful block of work. This way, threads won't sit on the
     * queue performing hundreds of useless CAS loops. This value should be a power of 2 so that the references don't
     * spill over a cache line, but it'll be platform specific, anyways.
     */
    private static final int BATCH_SIZE = 1<<7;

    /**
     * Assume we can use a latin-1 parser for all input types. This implementation is extremely efficient and should
     * work well for most North American locales.
     */
    private static final Latin1AddressParser ADDRESS_PARSER = new Latin1AddressParser();

    /**
     * Assume the source is in Latin-1, as it isn't otherwise specified. Given we're parsing addresses in the same
     * charset anyways, this shouldn't cause any real issues.
     */
    private static final String CHARSET = "ISO-8859-1";


    public static SortedMap<String, Integer> sortStreetsByProfitability(final InputStream parkingTicketsStream) {

        final RevenueCalculator c = new RevenueCalculator(CONSUMERS);

        try(final ReadableByteChannel rbc = Channels.newChannel(parkingTicketsStream);
            final Reader reader = Channels.newReader(rbc, CHARSET);
            final BufferedReader br = new BufferedReader(reader)){
            br.readLine(); // Consume the header.

            int i = 0;
            String[] lines = new String[BATCH_SIZE];
            TicketSource source = new TorontoCsvTicketSource(lines, ADDRESS_PARSER);
            while ((lines[i] = br.readLine()) != null) {
                i++;
                if(i >= BATCH_SIZE){
                    i = 0;
                    c.add(source);
                    lines = new String[BATCH_SIZE];
                    source = new TorontoCsvTicketSource(lines, ADDRESS_PARSER);
                }
            }
            c.add(source);

            return c.calculate(TIMEOUT_MINUTES);
        } catch(final IOException ex) { 
            throw new IllegalArgumentException("Failed to complete calculation. This must be remedied.", ex);
        }
    }
}
