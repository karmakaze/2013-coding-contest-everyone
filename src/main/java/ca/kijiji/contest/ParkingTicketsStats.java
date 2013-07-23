package ca.kijiji.contest;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Comparator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParkingTicketsStats {

	// 24-bit indices (16M possible entries)
	static final int BITS = 24;
	static final int UNUSED_BITS = 32 - BITS;
	static final int SIZE = 1 << BITS;
	static final int MASK = SIZE - 1;
	static final String[] keys = new String[SIZE];
	static final AtomicIntegerArray vals = new AtomicIntegerArray(SIZE);
	static byte[] data;

	public static int hash(final String k) {
		int h = 0;
		try {
			for (byte b : k.getBytes("UTF-8")) {
				int c = (b == ' ') ? 0 : (int)b & 0x00FF - 64;
				h = h * 71 + c;
				h = (h ^ (h >>> BITS)) & MASK;
			}
		}
		catch (UnsupportedEncodingException e) {}

		return h;
	}

	public static void add(final String k, final int d) {
		int i = hash(k);
		vals.addAndGet(i, d);

//		String k0 = keys[i];
//		if (k0 != null && !k0.equals(k)) {
//			System.err.println("Key hash clash: first "+ k0 +" and "+ k);
//		}
//		else {
			keys[i] = k;
//		}
	}
	public static int get(final String k) {
		int i = hash(k);
		return vals.get(i);
	}

    public static SortedMap<String, Integer> sortStreetsByProfitability(InputStream parkingTicketsStream) {
    	printInterval("Pre-entry initialization");

    	String name = "([A-Z][A-Z][A-Z]+|ST [A-Z][A-Z][A-Z]+)";
		Matcher nameMatcher = Pattern.compile(name).matcher("");

		ThreadGroup group = new ThreadGroup("workers");
		Runnable runnable = new Runnable() {
			public void run() {
			}};
		Thread t1 = new Thread(group, runnable);
		Thread t2 = new Thread(group, runnable);

		printProperty("os.arch");
    	System.out.println("InputStream is "+ parkingTicketsStream);
    	if (parkingTicketsStream instanceof BufferedInputStream) {
    		BufferedInputStream bis = (BufferedInputStream) parkingTicketsStream;
    	}

    	//final Map<String, Integer> streets = new HashMap<String, Integer>();
    	try {
    		int available = parkingTicketsStream.available();
    		System.out.println("Bytes available: "+ available);
    		data = new byte[available];
    		int a = 0;
    		for (int c = 16 * 4096; (c = parkingTicketsStream.read(data, a, c)) > 0; ) {
    			a += c;
    			if (available - a < c) {
    				c = available - a;
    			}
    		}

	    	printInterval("Local initialization: read "+ a +" bytes");

	    	BufferedReader r = new BufferedReader(new InputStreamReader(parkingTicketsStream));
	    	r.readLine();   // discard header row

	    	for(String line; (line = r.readLine()) != null; ) {
	    		String[] parts = line.split(",");
	    		try {
//		    		String tag_number_masked = parts[0];
//		    		String date_of_infraction = parts[1];
//		    		String infraction_code = parts[2];
//		    		String infraction_description = parts[3];
		    		String sfa = parts[4];
		    		Integer set_fine_amount = 0;
		    		try {
			    		set_fine_amount = Integer.parseInt(sfa);
		    		}
		    		catch (NumberFormatException e) {
		    			System.out.print(e.getClass().getSimpleName() +": "+ sfa);
		    		}
//		    		String time_of_infraction = parts[5];
//		    		String location1 = parts[6];
		    		String location2 = parts[7];
//		    		String location3 = parts[8];
//		    		String location4 = parts[9];
		    		nameMatcher.reset(location2);
		    		if (nameMatcher.find()) {
		    			String l = nameMatcher.group();
//	    			streetMatcher.reset(location2);
//	    			if (streetMatcher.find()) {
//	    				String l = streetMatcher.group(2);
	    			/*
			    	//	l = l.replaceAll("[0-9]+", "");
			    		l = l.replaceAll("[^A-Z]+ ", "");
			    		l = l.replaceAll(" (N|NORTH|S|SOUTH|W|WEST|E|EAST)$", "");
			    		l = l.replaceAll(" (AV|AVE|AVENUE|BLVD|CRES|COURT|CRT|DR|RD|ST|STR|STREET|WAY)$", "");
			    	//	l = l.replaceAll("^(A|M) ", "");
			    		l = l.replaceAll("(^| )(PARKING) .*$", "");
			    		l = l.trim();
		    		*/
//			    		String province = parts[10];
		    			add(l, set_fine_amount);

//		    			if (!l.equals("KING") && (location2.indexOf(" KING ") >= 0 || location2.endsWith(" KING"))) {
//		    				System.out.println(l +" <- "+ location2);
//		    			}
	    			}
	    			else {
	    				if (location2.indexOf("KING") >= 0 && location2.indexOf("PARKING") == -1) {
		    				System.out.println(""+ location2);
	    				}
	    			}
	    		}
	    		catch (ArrayIndexOutOfBoundsException e) {
	    			System.out.println(e.getClass().getSimpleName() +": "+ line);
	    		}
	    	}
    	}
    	catch (IOException e) {
			e.printStackTrace();
		}

    	printInterval("Read and summed");

//    	System.out.println("Size: "+ streets.size());

    	SortedMap<String, Integer> sorted = new TreeMap<String, Integer>(new Comparator<String>() {
			public int compare(String o1, String o2) {
				int c = get(o2) - get(o1);
				if (c != 0) return c;
				return o2.compareTo(o1);
			}});

    	for (int i = 0; i < SIZE; i++) {
    		for (int v; (v = vals.get(i)) != 0; ) {
    			sorted.put(keys[i], v);
    		}
    	}

    	printInterval("Populated TreeSet");

        return sorted;
    }

    static volatile long lastTime = System.currentTimeMillis();

    public static void printInterval(String name) {
    	long time = System.currentTimeMillis();
    	System.out.println(time +" "+ name +": "+ (time - lastTime) +" ms");
    	lastTime = time;
    }

    public static void printElement(String key, Map<String, Integer> streets) {
    	System.out.println(key +": $"+ streets.get(key));
    }

    public static void printProperty(String name) {
		System.out.println(name +": "+ System.getProperty(name));
    }
}