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

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/*
  	The programs takes around 790 millisecond on my laptop with Intel CORE i3 CPU.
  	Please run it as least five times to get the average running time.

	Here are some ways used to make this program run efficiently:
	1. Multi-threading.
	2. The size of buffer used to read data from file is set to 64K or a number greater than 16K 
	   in order to greatly reduce the number of function calling when reading file.
  	3. Scan the data of file at most byte by byte for once. For some cases, it can skip some bytes
  	   while parsing each line of the file.
  	4. Avoid frequently function calling when parsing each line of the file.
  	5. Avoid unnecessary memory copying.
  	6. Create a HashMap for each thread to store the result temporarily so that it avoids frequently 
  	   calling the expensive locking & unlocking commands for synchronized data.
  	7. Sort the map by values after parsing the file.
  	8. Optimize the program by the performance statistics of using different algorithms / data structures.  	
  	9. If the way to load file is not limited to only one InputStream, the performance of reading 
  	   the file can be improved greatly by using multi-threads and InputStream.skip() method.
  	  	
  	
  	Suggestions for making this contest more interesting and challenging:
  	1. Give more input files to analyze at the same time.
  	2. Reduce the memory size so that the memory cannot hold all the data.
  
  
  	If I luckily win a prize, could you give me the equivalent amount of money? I do not need any MacBook or iPad. :)
 */

public class ParkingTicketsStats {
	private static final Logger LOG = LoggerFactory.getLogger(ParkingTicketsStats.class);
	
    public static SortedMap<String, Integer> sortStreetsByProfitability(InputStream parkingTicketsStream) {
    	if (parkingTicketsStream == null) {
    		LOG.error("InputStream is null");
			return null;
		}

    	// create a HashMap to store the results
    	// this map will be filled with data from the thread's HashMap at the end of each thread's process
    	Map<String, Integer> ticketsStats = new HashMap<String, Integer>();

    	// convert the InputStream to BufferedInputStream which is thread safe
    	BufferedInputStream inStream = new BufferedInputStream(parkingTicketsStream);
    	
    	// create four threads since we have four CPU cores
    	Thread t1 = new Thread(new WorkThread(inStream, ticketsStats));
    	Thread t2 = new Thread(new WorkThread(inStream, ticketsStats));
    	Thread t3 = new Thread(new WorkThread(inStream, ticketsStats));
    	Thread t4 = new Thread(new WorkThread(inStream, ticketsStats));

    	t1.start();
    	t2.start();
    	t3.start();
    	t4.start();

    	try {
    		// wait for the exiting of the threads
			t1.join();
			t2.join();
			t3.join();
			t4.join();
    	} catch (InterruptedException e) {			
			LOG.error(e.getMessage());
		}
    	
    	/*
    	 	Here are some of the results in the map ordered by value:
                         YONGE,   3759440
                         QUEEN,   3389985
                         BLOOR,   3017500
                          KING,   2592080
                      DANFORTH,   1870110
                      ST CLAIR,   1850785
                       COLLEGE,   1738515
                      EGLINTON,   1705585
                      ADELAIDE,   1537280
                        DUNDAS,   1166165
                         FRONT,   1154955
                      RICHMOND,   1110685
                    WELLINGTON,   1085615
                      SHEPPARD,    952300
                       SPADINA,    914940
                    UNIVERSITY,    914655
    	 */    	
    	// In order to pass the test case of assertThat(streets.get(streets.firstKey()), closeTo(3781095)),
		// we might need to sort the map by their values from largest to smallest.    
    	// Here we implement our own TreeMap which has two important members:
    	// 1. HashMap which is used for operations like Map.get()
    	// 2. TreeSet which is used for operations like Map.firstKey(), Map.keySet()
       	TreeMapSortedByValue ticketsStatsSortedMap = new TreeMapSortedByValue(ticketsStats);
 		
        return ticketsStatsSortedMap;
    }
    
    

    
}


