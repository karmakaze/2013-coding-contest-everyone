package ca.kijiji.contest

import java.io.BufferedReader
import java.io.InputStream
import java.io.InputStreamReader
import java.util.Comparator
import java.util.Map
import java.util.SortedMap
import java.util.TreeMap
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern
import static extension java.lang.Integer.*

class ParkingTicketsStats {
	val static addr = new ConcurrentHashMap<String,Integer>
	
	def static SortedMap<String, Integer> sortStreetsByProfitability(InputStream parkingTicketsStream) {
		val reader = parkingTicketsStream.toBufferedReader
		reader.readLine // skip header
		reader.doLinesConcurrent[ var it = split(",")
			var street = extractStreet; var price = extractPrice
			addr.put(street, addr.get(street).add(price))
		]
		val sorted_map = new TreeMap(new ValueComparator(addr))
		sorted_map.putAll(addr)
		return sorted_map
	}
	
	def static doLinesConcurrent(BufferedReader reader, (String)=>void fn){
		var line = ""
		var s = Executors.newFixedThreadPool(4)
		while((line = reader.readLine) != null){
			s.execute(new LineProcessor(line, fn))
		}
		s.shutdown
		s.awaitTermination(10,TimeUnit.SECONDS)
	}
	
	def static toBufferedReader(InputStream stream){
		new BufferedReader(new InputStreamReader(stream))
	}
	
	val static trimPattern = Pattern.compile("(^[0-9]*\\s+|\\s(ST|AVE|SQ|RD"+
		"|BLVD|BRIDGE|BDGE|CRES|DR|ROAD|AV|QUAY|STREET|ST|DRIVE|DR|AVENUE|AVE|ROAD|RD"+
		"|CI?RC?L?E?|CIRCUIT|CRCT|CRESENT|CRES|MEWS|PARK|PK|PARKWAY|PATH|TRL|TRAIL|TER"+
		"|LOOP|COURT|CT|CIRCLE|LANE|LN|BOULEVARD|WAY|GREEN|PL|PLACE|GD|GDNS).*$)")
	def static extractStreet(String[] str){trimPattern.matcher(str.get(7)).replaceAll("")}
	def static extractPrice(String[] str){str.get(4).parseInt}
	def static add(Integer a, Integer b){if(a==null) b else a+b}
}

class LineProcessor implements Runnable{
	String line
	(String)=>void fn
	
	new(String line, (String)=>void fn){
		this.line = line
		this.fn = fn
	}
	
	override run() {fn.apply(line)}	
}

class ValueComparator implements Comparator<String> {
    Map<String, Integer> map
    new(Map<String, Integer> base) {this.map = base}
    override def int compare(String a, String b) {return map.get(b) - map.get(a)}
}