package ca.kijiji.contest;

import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;

public class StreetRegexTest {

    final static String[] testInputs = {
            "110 MARGUERETTA ST",
            "44 ABERDEEN AVE",
            "312 KING ST E",
            "521 ST CLAIR AVE W"
    };

    final static String[] testOutputs = {
            "MARGUERETTA",
            "ABERDEEN",
            "KING",
            "ST CLAIR"
    };

    @Test
    public void testCleanStreet() {
        String[] output = new String[testInputs.length];
        for (int i = 0; i < testInputs.length; i++) {
            output[i] = ParkingTicketsStats.cleanStreet(testInputs[i]);
        }
        assertArrayEquals(testOutputs, output);
    }

}
