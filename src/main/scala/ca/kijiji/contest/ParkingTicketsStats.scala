package ca.kijiji.contest

import scala.io.Source
import scala.collection.mutable
import com.google.common.base.Functions
import com.google.common.collect.{Ordering, ImmutableSortedMap}

object ParkingTicketsStats {

  def sortStreetsByProfitability(parkingTicketsStream: java.io.InputStream): java.util.SortedMap[java.lang.String, java.lang.Integer] = {
    //get an iterator of the lines in the file
    val iterator = Source.fromInputStream(parkingTicketsStream).getLines()
    //skip the first line (header)
    iterator.next()

    val hashMap = mutable.Map[String, Integer]()
    //iterate through lines, add the infractions
    iterator.foreach(i => {
      val infraction = Infraction.fromString(i)
      val previous : Integer = hashMap.getOrElse(infraction.street,0)
      hashMap.put(infraction.street, previous + infraction.amount)
    })

    val jMap = scala.collection.JavaConversions.mapAsJavaMap(hashMap)
    val comparator : Ordering[String] = Ordering.natural().onResultOf(Functions.forMap(jMap)).compound(Ordering.natural()).reverse()

    return ImmutableSortedMap.copyOf(jMap, comparator)
  }
}