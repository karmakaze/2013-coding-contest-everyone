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

class ParkingTicketsStats {
	var static addr = new ConcurrentHashMap<String,Integer>
	
	def static SortedMap<String, Integer> sortStreetsByProfitability(InputStream parkingTicketsStream) {
		val reader = parkingTicketsStream.toBufferedReader
		reader.readLine // skip header
		reader.linesConcurrent[
			var it = split(",")
			var old = addr.putIfAbsent(extractStreet, extractPrice)
			if(old!=null)addr.put(extractStreet,extractPrice+old)
		]
		val comp = new ValueComparator(addr)
		val sorted_map = new TreeMap(comp)
		sorted_map.putAll(addr)
		return sorted_map
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
	
	def static linesConcurrent(BufferedReader reader, (String)=>void fn){
		var line = ""
		var s = Executors.newFixedThreadPool(4);
		while((line = reader.readLine) != null){
			s.execute(new LineProcessor(line, fn));
		}
		s.shutdown
		s.awaitTermination(10,TimeUnit.SECONDS)
	}
}

class LineProcessor implements Runnable{
	String line
	(String)=>void fn
	
	new(String line, (String)=>void fn){
		this.line = line
		this.fn = fn
	}
	
	override run() {
		fn.apply(line)
	}
	
}

class ValueComparator implements Comparator<String> {
    Map<String, Integer> base;
    new(Map<String, Integer> base) {
        this.base = base;
    }
        
    override def int compare(String a, String b) {
        return base.get(b) - base.get(a);
    }
}