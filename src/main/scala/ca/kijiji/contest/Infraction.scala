package ca.kijiji.contest

import org.slf4j.Logger
import org.slf4j.LoggerFactory

object Infraction
{
  def fromString(line:String) : Infraction = {
    val csv = line.split(',')
    if(csv.length < 8)
      log.error("Cannot parse line : \n" + line)
    
    new Infraction(extractStreet(csv(StreetIndex)), csv(AmountIndex).toInt)
  }

  def extractStreet(address:String):String = {
    var arr = address.split(' ')
    if(arr.length > 1) {
      //if there is a number
      if(isAllDigits(arr(0)))
        arr = arr.drop(1)

      //if it ends with an orientation
      if(isOrientation(arr(arr.length - 1)))
        arr = arr.dropRight(2)
      //if not, the suffix
      else
        arr = arr.dropRight(1)
    }
    arr.mkString(" ")
  }

  private def isAllDigits(x: String) = x forall Character.isDigit
  private def isOrientation(x: String) = List("N", "S", "E", "W", "EAST", "WEST").contains(x)
  private final val StreetIndex = 7
  private final val AmountIndex = 4

  private final val log : Logger = LoggerFactory.getLogger(classOf[Infraction])
}

class Infraction(val street:String, val amount:Int)
{
  override def toString = "$" + amount + " on " + street
}

//end
//([A-Z]*\s)?(N|S|E|W)?$
//start
//^(\d*\s)?([A-Z]*)