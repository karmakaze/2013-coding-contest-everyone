package ca.kijiji.contest

import java.io.InputStream
import java.util.TreeMap
import java.io.BufferedReader
import java.io.InputStreamReader

class Main {
	var static addr = new TreeMap<String,Integer>
	
	def static sort(InputStream stream){
		val reader = stream.toBufferedReader
		reader.readLine //skip header
		println(reader.readLine)
		return addr
	}
	
	def static toBufferedReader(InputStream stream){
		new BufferedReader(new InputStreamReader(stream))
	}
}