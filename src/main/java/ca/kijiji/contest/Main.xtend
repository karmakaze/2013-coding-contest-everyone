package ca.kijiji.contest

import java.io.BufferedReader
import java.io.InputStream
import java.io.InputStreamReader
import java.util.Comparator
import java.util.Map
import java.util.Map.Entry
import java.util.SortedMap
import java.util.TreeMap
import java.util.regex.Pattern

class ParkingTicketsStats {
	var static addr = new TreeMap<String,Integer>
	
	def static SortedMap<String, Integer> sortStreetsByProfitability(InputStream parkingTicketsStream) {
		val reader = parkingTicketsStream.toBufferedReader
		reader.readLine // skip header
		reader.lines[
			var it = split(",")
			var old = addr.put(extractStreet, extractPrice)
			if(old!=null)addr.put(extractStreet,extractPrice+old)
		]
		return addr
	}
	
	def static toBufferedReader(InputStream stream){
		new BufferedReader(new InputStreamReader(stream))
	}
	
	def static toInt(String str){
		Integer.parseInt(str)
	}
	
	var static trimPattern = Pattern.compile("(^[0-9]*\\s+|\\s(ST|AVE|SQ|RD"+
		"|BLVD|BRIDGE|BDGE|CRES|DR|ROAD|AV|QUAY|STREET|ST|DRIVE|DR|AVENUE|AVE|ROAD|RD"+
		"|CI?RC?L?E?|CIRCUIT|CRCT|CRESENT|CRES|MEWS|PARK|PK|PARKWAY|PATH|TRL|TRAIL|TER"+
		"|LOOP|COURT|CT|CIRCLE|LANE|LN|BOULEVARD|WAY|GREEN|PL|PLACE|GD|GDNS).*$)")
	def static extractStreet(String[] str){ 
		trimPattern.matcher(str.get(7)).replaceAll("")
	}
	
	def static extractPrice(String[] str){
		str.get(4).toInt
	}
	
	def static lines(BufferedReader reader, (String)=>void fn){
		var line = ""
		while((line = reader.readLine) != null){fn.apply(line)}
	}
}

class ValueComparator implements Comparator<Map.Entry<String,Integer>>{	
	override compare(Entry<String,Integer> o1, Entry<String,Integer> o2) {
		return o1.value - o2.value
	}
}