package ca.kijiji.contest

import java.util.Date

class Infraction(val id:String, val date:String, val code:Int, val desc:String, val amt:Int,
                 val time:String, val loc1:String, val loc2:String, val loc3:String, val loc4:String, val prov:String)
{
  override def toString = "$" + amt + " at " + loc2

  def toParkingInfraction = new ParkingInfraction(Infraction.extractStreet(loc2),amt)
}

object Infraction
{
  def fromCSV(csv:Array[String]): Infraction = {
    println(csv.mkString(" "))
    new Infraction(csv(0),csv(1), csv(2).toInt, csv(3), csv(4).toInt, csv(5),
                   csv(6), csv(7), csv(8), csv(9), csv(10))
  }

  def fromString(line:String) : ParkingInfraction = {
    val csv = line.split(',')
    if(csv.length < 8)
      println(line)
    new ParkingInfraction(Infraction.extractStreet(csv(7)), csv(4).toInt)
  }

  def extractStreet(address:String):String = {
    var arr = address.split(' ')
    if(arr.length > 1)
    {
      if(isAllDigits(arr(0)))
        arr = arr.drop(1)
      if(isOrientation(arr(arr.length - 1)))
        arr = arr.dropRight(2)
      else
        arr = arr.dropRight(1)
    }
    arr.mkString(" ")
  }

  private def isAllDigits(x: String) = x forall Character.isDigit
  private def isOrientation(x: String) = List("N", "S", "E", "W", "EAST", "WEST").contains(x)
}

class ParkingInfraction(val street:String, val amount:Int)
{
  override def toString = "$" + amount + " on " + street
}

//end
//([A-Z]*\s)?(N|S|E|W)?$
//start
//^(\d*\s)?([A-Z]*)