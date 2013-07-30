package ca.kijiji.contest;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParkingTicketsStats {

	// 23-bit indices (8M possible entries)
	static final int BITS = 23;
	static final int UNUSED_BITS = 32 - BITS;
	static final int SIZE = 1 << BITS;
	static final int MASK = SIZE - 1;
	static final AtomicReferenceArray<String> keys = new AtomicReferenceArray<String>(SIZE);
	static final AtomicIntegerArray vals = new AtomicIntegerArray(SIZE);
	static volatile byte[] data;

	static final String name = "([A-Z][A-Z][A-Z]+|ST [A-Z][A-Z][A-Z]+)";
	static final Pattern namePattern = Pattern.compile(name);

	static volatile int available;

	static final ArrayBlockingQueue<Long> byteArrayQueue = new ArrayBlockingQueue<Long>(1024, true);
	static final AtomicInteger workItems = new AtomicInteger();

    public static SortedMap<String, Integer> sortStreetsByProfitability(InputStream parkingTicketsStream) {
    	printInterval("Pre-entry initialization");
/*
		printProperty("os.arch");
    	println("InputStream is "+ parkingTicketsStream);
    	if (parkingTicketsStream instanceof BufferedInputStream) {
    		BufferedInputStream bis = (BufferedInputStream) parkingTicketsStream;
    	}
*/
    	try {
			available = parkingTicketsStream.available();
    		println(System.currentTimeMillis(), "Bytes available: "+ available);

			ThreadGroup group = new ThreadGroup("workers");
			Runnable runnable = new Runnable() {
				public void run() {
					worker();
				}};
			int n = 7;
			Thread[] threads = new Thread[n];
			for (int k = 0; k < n; k++) {
				threads[k] = new Thread(group, runnable, Integer.toString(k), 1024);
			}

    		data = new byte[available];

    		workItems.incrementAndGet(); // 1 for first producer task

    		for (Thread t : threads) {
	    		t.start();
    		}

    		int a = 0;
    		int i = 0;
    		int j = 0;
    		for (int c = 32 * 1024 * 1024; (c = parkingTicketsStream.read(data, a, c)) > 0; ) {
    			a += c;
    			workItems.incrementAndGet();
    			i = j;
    			j = a;
    			if (a < available) {
    				while (data[--j] != '\n') {}
        			j++;
    			}

    			// don't offer the first (header) row
    			if (i == 0) {
    		    	printInterval("Local initialization: read first "+ a +" bytes");

    				while (data[i++] != '\n') {};
    			}

    			long ij = (long)i << 32 | (long)j & 0x0ffffffffL;
    			try {
					while (!byteArrayQueue.offer(ij, 1, TimeUnit.SECONDS)) {}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

    			if (available - a < c) {
    				c = available - a;
    			}
    		}

    		workItems.decrementAndGet();

	    	printInterval("Local initialization: read remaining of "+ a +" total bytes");

	    	for (Thread t: threads) {
	    		try {
					t.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
	    	}

	    	printInterval("All worker threads completed");
    	}
    	catch (IOException e) {
			e.printStackTrace();
		}

    	printInterval("Read and summed");

//    	println("Size: "+ streets.size());

    	final SortedMap<String, Integer> sorted = new TreeMap<String, Integer>(new Comparator<String>() {
			public int compare(String o1, String o2) {
				int c = get(o2) - get(o1);
				if (c != 0) return c;
				return o2.compareTo(o1);
			}});

    	final int B = SIZE / 2;
//    	final int C = B + B + 1;

    	Thread t0 = new Thread(null, null, "g0", 1024) {
    		public void run() {
    	    	for (int i = 0; i < B; i++) {
    	    		int v = vals.get(i);
    	    		if (v != 0) {
    	    			synchronized (sorted) {
    		    			sorted.put(keys.get(i), v);
    	    			}
    	    		}
    	    	}
    		}
    	};
    	t0.start();

    	Thread t1 = new Thread(null, null, "g1", 1024) {
    		public void run() {
    	    	for (int i = B; i < SIZE; i++) {
    	    		int v = vals.get(i);
    	    		if (v != 0) {
    	    			synchronized (sorted) {
    		    			sorted.put(keys.get(i), v);
    	    			}
    	    		}
    	    	}
    		}
    	};
    	t1.start();

    	try { t0.join(); } catch (InterruptedException e) {}
    	try { t1.join(); } catch (InterruptedException e) {}
//    	try { t2.join(); } catch (InterruptedException e) {}

    	printInterval("Populated TreeSet");

        return sorted;
    }

    /**
     * worker parallel worker takes blocks of bytes read and processes them
     */
    static final void worker() {
		try {
			String threadName = Thread.currentThread().getName();
			Matcher nameMatcher = namePattern.matcher("");

			// local access faster than volatile fields
			byte[] data = ParkingTicketsStats.data;

			final ArrayList<String> parts = new ArrayList<>();

			int work = 0;
			do {
				Long ij = byteArrayQueue.poll(5, TimeUnit.MILLISECONDS);
				if (ij != null) {
					int i = (int) (ij >>> 32);
					int j = (int) (long) ij;
				//	println("Thread ["+ threadName +"] processing block("+ i +", "+ j +")");

					// process block
					for (int m; i < j; i = m) {
						// process a line
						m = i;
						while (m < j && data[m++] != (byte)'\n') {}

						parts.clear();
						int k;
						int c = 0;
						do {
							k = i;
							while (k < m && data[k] != ',' && data[k] != '\n') { k++; }
							if (c == 4 || c == 7) {
								parts.add(new String(data, i, k - i));
							} else {
								parts.add(null);
							}
							c++;
							i = k + 1;
						} while (i < m);

			    		try {
	//			    		String tag_number_masked = parts[0];
	//			    		String date_of_infraction = parts[1];
	//			    		String infraction_code = parts[2];
	//			    		String infraction_description = parts[3];
				    		String sfa = parts.get(4);
				    		Integer set_fine_amount = 0;
				    		try {
					    		set_fine_amount = Integer.parseInt(sfa);
				    		}
				    		catch (NumberFormatException e) {
				    			System.out.print(e.getClass().getSimpleName() +": "+ sfa);
				    		}
	//			    		String time_of_infraction = parts[5];
	//			    		String location1 = parts[6];
				    		String location2 = parts.get(7);
	//			    		String location3 = parts[8];
	//			    		String location4 = parts[9];
				    		nameMatcher.reset(location2);
				    		if (nameMatcher.find()) {
				    			String l = nameMatcher.group();
	//		    			streetMatcher.reset(location2);
	//		    			if (streetMatcher.find()) {
	//		    				String l = streetMatcher.group(2);
			    			/*
					    	//	l = l.replaceAll("[0-9]+", "");
					    		l = l.replaceAll("[^A-Z]+ ", "");
					    		l = l.replaceAll(" (N|NORTH|S|SOUTH|W|WEST|E|EAST)$", "");
					    		l = l.replaceAll(" (AV|AVE|AVENUE|BLVD|CRES|COURT|CRT|DR|RD|ST|STR|STREET|WAY)$", "");
					    	//	l = l.replaceAll("^(A|M) ", "");
					    		l = l.replaceAll("(^| )(PARKING) .*$", "");
					    		l = l.trim();
				    		*/
	//				    		String province = parts[10];
				    			add(l, set_fine_amount);

	//			    			if (!l.equals("KING") && (location2.indexOf(" KING ") >= 0 || location2.endsWith(" KING"))) {
	//			    				println(l +" <- "+ location2);
	//			    			}
			    			}
			    			else {
			    				if (location2.indexOf("KING") >= 0 && location2.indexOf("PARKING") == -1) {
				    				println(""+ location2);
			    				}
			    			}
			    		}
			    		catch (ArrayIndexOutOfBoundsException e) {
			    			println(e.getClass().getSimpleName() +": "+ parts);
			    			e.printStackTrace();
			    		}
					}
					work = workItems.decrementAndGet();
				}
				else {
					work = workItems.get();
				}
			//	println("Thread ["+ threadName +"] work remaining "+ work +" queued="+ byteArrayQueue.size());
			} while (work > 0);
			println(System.currentTimeMillis(), "Thread ["+ threadName +"] ending normally");
		}
		catch (InterruptedException e) {
			e.printStackTrace();
		}
    }

	public static int hash(final String k) {
		int h = 0;
		try {
			for (byte b : k.getBytes("UTF-8")) {
				if (h < 0 || h > MASK) {
					h = (h & MASK) ^ (h >>> BITS);
				}
				int c = (b == ' ') ? 0 : (int)b & 0x00FF - 64;
				h = h * 47 + c;
			}
		}
		catch (UnsupportedEncodingException e) {}

		return h & MASK;
	}

	public static void add(final String k, final int d) {
		int i = hash(k);

		if (vals.getAndAdd(i, d) == 0) {
			keys.set(i, k);
		}
		// uncomment below to print hash collisions
//		else {
//			String k0 = keys.getAndSet(i, k);
//			if (!k.equals(k0)) {
//				println("Key hash clash: first "+ k +" and "+ k0);
//			}
//		}
	}
	public static int get(final String k) {
		int i = hash(k);
		return vals.get(i);
	}

    static volatile long lastTime = System.currentTimeMillis();

    public static void printInterval(String name) {
    	long time = System.currentTimeMillis();
    	println(time, name +": "+ (time - lastTime) +" ms");
    	lastTime = time;
    }

    public static void printElement(String key, Map<String, Integer> streets) {
    	println(key +": $"+ streets.get(key));
    }

    public static void printProperty(String name) {
		println(name +": "+ System.getProperty(name));
    }

    public static void println(long time, String line) {
    	println(time%10000 +" "+ line);
    }

    public static void println(String line) {
    	System.out.println(line);
    }
}