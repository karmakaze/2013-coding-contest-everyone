/**
 * Kijiji programming contest July 2013:
 * http://kijijiblog.ca/so-you-think-you-can-code-eh/
 * 
 * Author: Yuan (yuan.java at gmail.com)
 * 
 * Licensed under Apache License v2.0
 *  
 */

package ca.kijiji.contest;

import java.io.InputStream;
import java.util.Map;

public class WorkThread implements Runnable {
	InputStream inStream = null;
	Map<String, Integer> ticketsStats = null;
	
	public WorkThread(InputStream inStream, Map<String, Integer> ticketsStats) {
		this.inStream = inStream;
		this.ticketsStats = ticketsStats;
	}

	@Override
	public void run() {
		FileParser fileParser = new FileParser(inStream, ticketsStats);
		
		fileParser.parseFile();

	}

}
