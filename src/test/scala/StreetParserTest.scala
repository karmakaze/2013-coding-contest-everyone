import ca.kijiji.contest.Infraction
import org.junit.Test
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.equalTo

class StreetParserTest {

  @Test def ShouldParseStreetWithNumberAndName(){
    val address = "1040 THE QUEENSWAY"
    val street = Infraction.extractStreet(address)
    assertThat(street, equalTo("THE QUEENSWAY"))
  }

  @Test def ShouldParseStreetWithNumberAndNameAndSuffix(){
    val address = "109 OLD FOREST HILL RD"
    val street = Infraction.extractStreet(address)
    assertThat(street, equalTo("OLD FOREST HILL"))
  }

  @Test def ShouldParseStreetWithNameAndSuffix(){
    val address = "ST CLARENS AVE"
    val street = Infraction.extractStreet(address)
    assertThat(street, equalTo("ST CLARENS"))
  }

  @Test def ShouldParseStreetWithNumberAndNameAndSuffixAndDirection(){
    val address = "12 ST CLAIR AVE E"
    val street = Infraction.extractStreet(address)
    assertThat(street, equalTo("ST CLAIR"))
  }

  @Test def ShouldParseStreetWithNameAndSuffixAndDirection(){
    val address = "ST CLAIR AVE W"
    val street = Infraction.extractStreet(address)
    assertThat(street, equalTo("ST CLAIR"))
  }
}

//44 O'LEARY AVE
//625 FINCH AVE WEST
//ST ENOCH'S SQ

