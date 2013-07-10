package ca.burnison.kijiji.contest;

import static org.mockito.Mockito.*;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import jsr166e.LongAdder;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.Lists;


@RunWith(MockitoJUnitRunner.class)
public final class RevenueTaskTest
{
    private static final String STREET = "king";
    private static final int AMOUNT = 160;

    private RevenueTask task;
    private ConcurrentMap<String, LongAdder> map;

    @Mock private TicketSource SOURCE;
    @Mock private AddressParser PARSER;

    @Before
    public void setup()
    {
        this.map = new ConcurrentHashMap<>();
        this.task = new RevenueTask(map, SOURCE);
    }

    @Test
    public void testRunShouldAddAmount()
    {
        setupIterator(new Ticket(STREET, AMOUNT));
        this.task.run();
        Assert.assertEquals(AMOUNT, this.map.get(STREET).intValue());
    }

    @Test
    public void testRunTwiceShouldDoubleAmount()
    {
        setupIterator(new Ticket(STREET, AMOUNT), new Ticket(STREET, AMOUNT));
        this.task.run();
        this.task.run();
        Assert.assertEquals(AMOUNT * 2, this.map.get(STREET).intValue());
    }

    @Test
    public void runWithNullStreetShouldNotAddToMap()
    {
        setupIterator(new Ticket(null, AMOUNT));
        this.task.run();
        Assert.assertNull(this.map.get(STREET));
    }

    @Test
    public void runWithNullTicketShouldNotAddToMap()
    {
        setupIterator((Ticket)null);
        this.task.run();
        Assert.assertNull(this.map.get(STREET));
    }

    private void setupIterator(final Ticket ticket, final Ticket... tickets)
    {
        when(SOURCE.iterator()).thenReturn(Lists.asList(ticket, tickets).iterator());
    }
}
