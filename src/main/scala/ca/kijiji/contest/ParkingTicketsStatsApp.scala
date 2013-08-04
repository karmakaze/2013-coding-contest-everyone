package ca.kijiji.contest

import java.io._
import java.util.{Comparator, TreeMap}

import scala.io.Source
import scala.concurrent.{future, promise}
import scala.collection.JavaConversions._

import ca.kijiji.contest.parsers.LocationParser


object ParkingTicketsStatsApp{
  def getSortedMap(fis:InputStream) = {
    val seedMap = Map[String, Integer]()
    val comboCombineOld = (map1:Map[String, Integer], map2:Map[String, Integer])=>{
      val list = map1.toList ++ map2.toList
      val merged = list.groupBy ( _._1) .map { case (k,v) => k -> v.foldLeft(0:Integer)((acc, current)=>{
        (acc + current._2):Integer
      })}
      merged
    }
    val comboCombine = (map1:Map[String, Integer], map2:Map[String, Integer])=>{
      if(map1.size > map2.size){
        map2.foldLeft(map1)((currentMap, item) =>{
          val currentAmount:Integer = currentMap.getOrElse(item._1, 0:Integer)
          val newAmount:Integer = currentAmount + item._2
          currentMap + (item._1 -> newAmount)
        })
      }
      else{
        map1.foldLeft(map2)((currentMap, item) =>{
          val currentAmount:Integer = currentMap.getOrElse(item._1, 0:Integer)
          val newAmount:Integer = currentAmount + item._2
          currentMap + (item._1 -> newAmount)
        })
      }
    }

    val seqCombine = (currentMap:Map[String, Integer], currentLine:String) => {
      val tokens = currentLine.split(",")
      val amount = tokens(4).toInt
      val location = LocationParser.parse(tokens(7)).mkString(" ")
      val currentAmount:Integer = currentMap.getOrElse(location, 0:Integer)
      val newAmount:Integer = currentAmount + amount
      currentMap + (location -> newAmount)
    }


    val startTime = System.currentTimeMillis();
    //val reducedMap = Source.fromInputStream(fis).getLines.toSeq.tail.par.aggregate(seedMap)(seqCombine, comboCombine)
    val reducedMap = Source.fromInputStream(fis).getLines.toSeq.tail.foldLeft(seedMap)(seqCombine) 
    val duration = System.currentTimeMillis() - startTime;
    println("CONSOLIDATION: "+ duration);
    val startMap:TreeMap[String, Integer]= new TreeMap(new ValueCamparator(reducedMap))
    startMap.putAll(reducedMap:java.util.Map[String, Integer])
    startMap
  }

  class ValueCamparator(dataMap:Map[String, Integer]) extends Comparator[String]{
    def compare(o1:String, o2:String)={
      val result = dataMap(o2).compareTo(dataMap(o1))
      if(result == 0){
         o1.compareTo(o2)
      }
      else{
        result
      }
    }
  }
}

