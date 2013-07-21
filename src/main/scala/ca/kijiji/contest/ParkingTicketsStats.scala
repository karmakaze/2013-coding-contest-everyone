package ca.kijiji.contest

import scala.io.Source
import java.util

object ParkingTicketsStats {
  def sortStreetsByProfitability(parkingTicketsStream: java.io.InputStream): java.util.SortedMap[java.lang.String, java.lang.Integer] = {
    //get an iterator of the lines in the file
    val iterator = Source.fromInputStream(parkingTicketsStream).getLines()
    //skip the first line
    iterator.next()

    //iterate through lines
    iterator.foreach(i => println(Infraction.fromCSV(i.split(',')).toParkingInfraction.toString))

    return new util.TreeMap()
  }
}