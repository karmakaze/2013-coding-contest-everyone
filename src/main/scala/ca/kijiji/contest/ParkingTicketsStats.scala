package ca.kijiji.contest

import scala.io.Source
import java.util

object ParkingTicketsStats {
  def sortStreetsByProfitability(parkingTicketsStream: java.io.InputStream): java.util.SortedMap[java.lang.String, java.lang.Integer] = {
    //get an iterator of the lines in the file
    val iterator = Source.fromInputStream(parkingTicketsStream).getLines()
    //skip the first line
    iterator.next()

    val hashMap = new scala.collection.concurrent.TrieMap[String, Integer]()
    //iterate through lines
    iterator.foreach(i => {
      val inf = Infraction.fromString(i)
      val prev : Integer = hashMap.getOrElse(inf.street, 0)
      hashMap.put(inf.street, prev + inf.amount)
    })

    return new java.util.TreeMap[String, Integer](scala.collection.JavaConversions.mapAsJavaMap(hashMap))
  }
}