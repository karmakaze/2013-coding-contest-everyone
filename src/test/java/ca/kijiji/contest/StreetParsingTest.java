package ca.kijiji.contest;

import junit.framework.TestCase;

import org.junit.Test;

import ca.kijiji.contest.exceptions.UnparseableLocationException;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class StreetParsingTest extends TestCase {

	@Test
	public void testTwoWordStreetWithAllParts() throws UnparseableLocationException {
		streetTestHelper("1234 ST CLAIR AVE E", "ST CLAIR");
	}
	
	@Test
	public void testOneWordStreetWithAllParts() throws UnparseableLocationException {
		streetTestHelper("1234 QUEEN ST E", "QUEEN");
	}
	
	@Test
	public void testNumberedStreetWithAddress() throws UnparseableLocationException {
		streetTestHelper("1234 3RD ST", "3RD");
	}

	@Test
	public void testNumberedStreetWithoutAddress() throws UnparseableLocationException {
		streetTestHelper("2ND ST", "2ND");
	}
	
	@Test
	public void testTwoWordStreetWithoutNumber() throws UnparseableLocationException {
		streetTestHelper("ST CLAIR AV EAST", "ST CLAIR");
	}

	// Not using @Test(expected=...) because it's not working for some reason.
	@Test
	public void testBlankLocation() {
		try {
			StreetUtil.parseStreet("");
			fail("UnparseableLocationException not thrown.");
		}
		catch (UnparseableLocationException ule) {
			
		}
	}
	
	@Test
	public void testSymbolLocation() {
		try {
			StreetUtil.parseStreet(".");
			fail("UnparseableLocationException not thrown.");
		}
		catch (UnparseableLocationException ule) {
			
		}
	}
	
	@Test
	public void testFatFingerSuffix() throws UnparseableLocationException {
		streetTestHelper("ST CLAIRA VE EAST", "ST CLAIR");
	}

	@Test
	public void testFatFingerSuffix2() throws UnparseableLocationException {
		streetTestHelper("FINCHA VE EAST", "FINCH");
	}
	
	@Test
	public void testOneWordStreet() throws UnparseableLocationException {
		streetTestHelper("QUEENSWAY", "QUEENSWAY");
	}

	public void streetTestHelper(String location, String expected) throws UnparseableLocationException {
		String actual = StreetUtil.parseStreet(location);
		assertThat(actual, equalTo(expected));
	}
}