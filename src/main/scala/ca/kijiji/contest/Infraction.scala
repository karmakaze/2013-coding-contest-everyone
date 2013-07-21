package ca.kijiji.contest

import java.util.Date

class Infraction(val id:String, val date:String, val code:Int, val desc:String, val amt:Int,
                 val time:String, val loc1:String, val loc2:String, val loc3:String, val loc4:String, val prov:String)
{
  override def toString = "$" + amt + " at " + loc2

  def toParkingInfraction = new ParkingInfraction(extractStreet(loc2),amt)

  private def extractStreet(loc2:String):String =  {
    val arr = loc2.split(' ')
    if(isAllDigits(arr(0)))
      arr(1) + " " + arr(2)
    else
      arr(0)
  }
  private def isAllDigits(x: String) = x forall Character.isDigit
}

object Infraction
{
  def fromCSV(csv:Array[String]): Infraction =
    new Infraction(csv(0),csv(1), csv(2).toInt, csv(3), csv(4).toInt, csv(5),
                   csv(6), csv(7), csv(8), csv(9), csv(10))

  def extractStreet(address:String):String = {
    val arr = address.split(' ')
    if(isAllDigits(arr(0)))
      arr(1) + " " + arr(2)
    else
      arr(0)
  }

  private def isAllDigits(x: String) = x forall Character.isDigit
}

class ParkingInfraction(val street:String, val amount:Int)
{
  override def toString = "$" + amount + " on " + street
}

//end
//([A-Z]*\s)?(N|S|E|W)?$
//start
//^(\d*\s)?([A-Z]*)