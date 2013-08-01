package ca.kijiji.contest;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test for StreetNameParser
 * <p/>
 * User: Eugene Loykov
 * Date: 13.07.13
 * Time: 20:00
 */
public final class StreetNameParseTest {
    @Test
    public void streetNameOnly() {
        final String testStr = "STREET NAME";
        Assert.assertEquals("STREET NAME", StreetNameParser.parse(toRawBytes(testStr), 0, testStr.length() - 1));
    }

    @Test
    public void houseNumberDeletion() {
        final String testStr = "42 % GOD SAVE THE QUEEN";
        Assert.assertEquals("GOD SAVE THE QUEEN", StreetNameParser.parse(toRawBytes(testStr), 0, testStr.length() - 1));
    }

    @Test
    public void houseNumberDeletion2() {
        final String testStr = "42 QUE42EN ST W";
        Assert.assertEquals("QUE42EN", StreetNameParser.parse(toRawBytes(testStr), 0, testStr.length() - 1));
    }

    @Test
    public void suffixDirectionDeletion() {
        final String testStr = "QUEEN ST W";
        Assert.assertEquals("QUEEN", StreetNameParser.parse(toRawBytes(testStr), 0, testStr.length() - 1));
    }

    @Test
    public void suffixDirectionDeletion2() {
        final String testStr = "QUEEN 4ST EAST";
        Assert.assertEquals("QUEEN 4ST", StreetNameParser.parse(toRawBytes(testStr), 0, testStr.length() - 1));
    }

    @Test
    public void suffixDirectionDeletion3() {
        final String testStr = "QUEEN OF ST JAVA EAST";
        Assert.assertEquals("QUEEN OF ST JAVA", StreetNameParser.parse(toRawBytes(testStr), 0, testStr.length() - 1));
    }

    @Test
    public void complexDeletion() {
        final String testStr = "610 QUEEN ST W";
        Assert.assertEquals("QUEEN", StreetNameParser.parse(toRawBytes(testStr), 0, testStr.length() - 1));
    }

    private static byte[] toRawBytes(String string) {
        byte[] bytes = new byte[string.length()];

        for (int i = 0; i < string.length(); i++) {
            bytes[i] = (byte) string.charAt(i);
        }

        return bytes;
    }
}
