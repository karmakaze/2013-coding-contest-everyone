package ca.burnison.kijiji.contest;

import static org.mockito.Mockito.*;

import java.util.SortedMap;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.Lists;

@RunWith(MockitoJUnitRunner.class)
public final class RevenueCalculatorTest
{
    private static final Ticket TICKET_1 = new Ticket("a", 22);
    private static final Ticket TICKET_2 = new Ticket("b", 33);
    private static final Ticket TICKET_3 = new Ticket("b", 44);
    private static final Ticket TICKET_4 = new Ticket("c", 76);
    private static final Ticket TICKET_5 = new Ticket("d", 76);

    @Mock private TicketSource SOURCE;

    @Test(timeout = 1000)
    public void testSimple()
    {
        when(SOURCE.iterator()).thenReturn(Lists.asList(TICKET_1, new Ticket[]{TICKET_2, TICKET_3, TICKET_4, TICKET_5}).iterator());

        final RevenueCalculator c = new RevenueCalculator(1);
        c.add(SOURCE);

        final SortedMap<String, Integer> calculated = c.calculate(1);
        Assert.assertEquals(4, calculated.size());
        Assert.assertEquals("b", calculated.firstKey());
        Assert.assertEquals("a", calculated.lastKey());
        Assert.assertEquals(77, calculated.get("b").intValue());
        Assert.assertEquals(76, calculated.get("c").intValue());
        Assert.assertEquals(22, calculated.get("a").intValue());
        Assert.assertEquals(76, calculated.get("d").intValue());
        Assert.assertNull(calculated.get("m"));
    }

    @Test(timeout = 1000)
    public void testClose()
    {
        final RevenueCalculator c = new RevenueCalculator(1);
        c.calculate(1);
        c.calculate(1);
    }

    @Test(timeout = 1000, expected = IllegalStateException.class)
    public void testAddAfterClose()
    {
        final RevenueCalculator c = new RevenueCalculator(1);
        c.calculate(1);
        c.add(SOURCE);
    }
}
