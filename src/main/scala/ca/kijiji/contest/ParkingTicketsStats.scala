package ca.kijiji.contest

import java.util.concurrent.ConcurrentHashMap
import scala.io.Source
import com.google.common.base.Functions
import com.google.common.collect.{Ordering, ImmutableSortedMap}

object ParkingTicketsStats {

  def sortStreetsByProfitability(parkingTicketsStream: java.io.InputStream): java.util.SortedMap[String, Integer] = {
    //get an iterator of the lines in the file
    val iterator = Source.fromInputStream(parkingTicketsStream).getLines()
    val hashMap = new ConcurrentHashMap[String, Integer]()

    try {
      //skip the first line (header)
      iterator.next()

      //iterate through lines in chunks
      for ( chunk <- iterator.grouped(ChunkSize) ) {
        //add the infractions in parallel
        chunk.par.foreach(line => {
          val infraction = Infraction.fromString(line)
          val previous: Integer = Option(hashMap.get(infraction.street)).getOrElse(0)
          hashMap.put(infraction.street, previous + infraction.amount)
        })
      }
    } finally {
      parkingTicketsStream.close()
    }

    //sort by value, descending
    val comparator: Ordering[String] = Ordering.natural().onResultOf(Functions.forMap(hashMap)).compound(Ordering.natural()).reverse()
    ImmutableSortedMap.copyOf(hashMap, comparator)
  }

  private final val ChunkSize = 9600
}