package ca.burnison.kijiji.contest;

import org.junit.Assert;
import org.junit.Test;

public final class Latin1AddressParserTest
{
    private final Latin1AddressParser parser = new Latin1AddressParser();

    @Test
    public void streetShouldBeNullWhenAddresNullOrEmpty()
    {
        Assert.assertNull(this.parser.parse(null));
        Assert.assertNull(this.parser.parse(""));
        Assert.assertNull(this.parser.parse("   "));
    }

    @Test
    public void normalizeToNull()
    {
        Assert.assertNull(this.parser.parse("111"));
    }

    @Test
    public void streetShouldBeSameButNormalizedWhenAddressHasNoSpace()
    {
        Assert.assertEquals("KING", this.parser.parse("king"));
    }

    @Test
    public void streetShouldHaveNumberRemoved()
    {
        Assert.assertEquals("KING", this.parser.parse("123 king"));
        Assert.assertEquals("KING", this.parser.parse("1-23 king"));
        Assert.assertEquals("KING", this.parser.parse("123a king"));
        Assert.assertEquals("KING", this.parser.parse("a123 king"));
        Assert.assertEquals("L'EAT", this.parser.parse("12/3 l'eat"));
    }

    @Test
    public void streetShouldHaveDirectionRemoved()
    {
        Assert.assertEquals("KING", this.parser.parse("123 king w"));
        Assert.assertEquals("KING", this.parser.parse("123 king west"));
        Assert.assertEquals("KING", this.parser.parse("123 king e"));
        Assert.assertEquals("KING", this.parser.parse("123 king east"));
        Assert.assertEquals("KING", this.parser.parse("123 king s"));
        Assert.assertEquals("KING", this.parser.parse("123 king south"));
        Assert.assertEquals("KING", this.parser.parse("123 king n"));
        Assert.assertEquals("KING", this.parser.parse("123 king north"));
    }

    @Test
    public void streetShouldntHaveUnitLetter()
    {
        Assert.assertEquals("KING", this.parser.parse("123a king street west"));
        Assert.assertEquals("KING", this.parser.parse("b123 king street west"));
    }

    @Test
    public void streetShouldHaveTypeRemoved()
    {
        Assert.assertEquals("ST CLAIR", this.parser.parse("123 st clair st west"));
        Assert.assertEquals("ST CLAIR", this.parser.parse("123 st clair street west"));

        Assert.assertEquals("ST CLAIR", this.parser.parse("123 st clair sq west"));
        Assert.assertEquals("ST CLAIR", this.parser.parse("123 st clair square west"));

        Assert.assertEquals("ST CLAIR", this.parser.parse("123 st clair av west"));
        Assert.assertEquals("ST CLAIR", this.parser.parse("123 st clair ave west"));
        Assert.assertEquals("ST CLAIR", this.parser.parse("123 st clair avenue west"));

        Assert.assertEquals("ST CLAIR", this.parser.parse("123 st clair dr west"));
        Assert.assertEquals("ST CLAIR", this.parser.parse("123 st clair drive west"));

        Assert.assertEquals("ST CLAIR", this.parser.parse("123 st clair rd west"));
        Assert.assertEquals("ST CLAIR", this.parser.parse("123 st clair road west"));

        Assert.assertEquals("ST CLAIR", this.parser.parse("123 st clair bvd west"));
        Assert.assertEquals("ST CLAIR", this.parser.parse("123 st clair blvd west"));
        Assert.assertEquals("ST CLAIR", this.parser.parse("123 st clair boul west"));
        Assert.assertEquals("ST CLAIR", this.parser.parse("123 st clair boulevard west"));

        Assert.assertEquals("ST CLAIR", this.parser.parse("123 st clair ci west"));
        Assert.assertEquals("ST CLAIR", this.parser.parse("123 st clair cr west"));
        Assert.assertEquals("ST CLAIR", this.parser.parse("123 st clair cir west"));
        Assert.assertEquals("ST CLAIR", this.parser.parse("123 st clair circ west"));
        Assert.assertEquals("ST CLAIR", this.parser.parse("123 st clair crcl west"));
        Assert.assertEquals("ST CLAIR", this.parser.parse("123 st clair circle west"));
        Assert.assertEquals("ST CLAIR", this.parser.parse("123 st clair cle west"));
        Assert.assertEquals("ST CLAIR", this.parser.parse("123 st clair cr west"));
        Assert.assertEquals("ST CLAIR", this.parser.parse("123 st clair crs west"));
        Assert.assertEquals("ST CLAIR", this.parser.parse("123 st clair cres west"));
        Assert.assertEquals("ST CLAIR", this.parser.parse("123 st clair cresent west"));
        Assert.assertEquals("ST CLAIR", this.parser.parse("123 st clair crescent west"));
        Assert.assertEquals("ST CLAIR", this.parser.parse("123 st clair ct west"));
        Assert.assertEquals("ST CLAIR", this.parser.parse("123 st clair crt west"));
        Assert.assertEquals("ST CLAIR", this.parser.parse("123 st clair court west"));

        Assert.assertEquals("ST CLAIR", this.parser.parse("123 st clair la west"));
        Assert.assertEquals("ST CLAIR", this.parser.parse("123 st clair ln west"));
        Assert.assertEquals("ST CLAIR", this.parser.parse("123 st clair lane west"));
        Assert.assertEquals("ST CLAIR", this.parser.parse("123 st clair lp west"));
        Assert.assertEquals("ST CLAIR", this.parser.parse("123 st clair loop west"));

        Assert.assertEquals("ST CLAIR", this.parser.parse("123 st clair tr west"));
        Assert.assertEquals("ST CLAIR", this.parser.parse("123 st clair ter west"));
        Assert.assertEquals("ST CLAIR", this.parser.parse("123 st clair terr west"));
        Assert.assertEquals("ST CLAIR", this.parser.parse("123 st clair terrace west"));

        Assert.assertEquals("ST CLAIR", this.parser.parse("123 st clair pl west"));
        Assert.assertEquals("ST CLAIR", this.parser.parse("123 st clair place west"));

        Assert.assertEquals("ST CLAIR", this.parser.parse("123 st clair pk west"));
        Assert.assertEquals("ST CLAIR", this.parser.parse("123 st clair pkw west"));
        Assert.assertEquals("ST CLAIR", this.parser.parse("123 st clair prk west"));
        Assert.assertEquals("ST CLAIR", this.parser.parse("123 st clair pkwy west"));

        Assert.assertEquals("ST CLAIR", this.parser.parse("123 st clair gdn west"));
        Assert.assertEquals("ST CLAIR", this.parser.parse("123 st clair gds west"));
        Assert.assertEquals("ST CLAIR", this.parser.parse("123 st clair gdns west"));
        Assert.assertEquals("ST CLAIR", this.parser.parse("123 st clair gardens west"));
    }
}
