package ca.kijiji.contest;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.util.Iterator;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import ca.burnison.kijiji.contest.AddressParser;
import ca.burnison.kijiji.contest.Ticket;

@RunWith(MockitoJUnitRunner.class)
public final class TorontoCsvTicketSourceTest
{
    private static final String[] CSV = { "1,20,30,40,50,60,70,80 KING STREET WEST,100,110,120" };
    private static final String STREET = "street";

    @Mock private AddressParser PARSER;

    @Before
    public void setup()
    {
        when(PARSER.parse(anyString())).thenReturn(STREET);
    }

    @Test
    public void parseShouldWork()
    {
        final TorontoCsvTicketSource source = new TorontoCsvTicketSource(CSV, PARSER);
        final Iterator<Ticket> i = source.iterator();
        Assert.assertTrue(i.hasNext());
        
        final Ticket ticket = i.next();
        Assert.assertEquals(50, ticket.getAmount());
        Assert.assertEquals(STREET, ticket.getStreet());
        
        Assert.assertFalse(i.hasNext());
    }

    @Test
    public void parseShouldReturnNullForInvalidArrays()
    {
        final TorontoCsvTicketSource source = new TorontoCsvTicketSource(new String[]{",,"}, PARSER);
        final Iterator<Ticket> i = source.iterator();
        Assert.assertTrue(i.hasNext());
        Assert.assertNull(i.next());
    }


    @Test(expected = UnsupportedOperationException.class)
    public void testRemoveUnsupported()
    {
        new TorontoCsvTicketSource(CSV, PARSER).iterator().remove();
    }
}
