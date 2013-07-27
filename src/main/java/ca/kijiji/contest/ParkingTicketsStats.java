package ca.kijiji.contest;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Comparator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParkingTicketsStats {

	// 24-bit indices (16M possible entries)
	static final int BITS = 24;
	static final int UNUSED_BITS = 32 - BITS;
	static final int SIZE = 1 << BITS;
	static final int MASK = SIZE - 1;
	static final AtomicReferenceArray<String> keys = new AtomicReferenceArray<String>(SIZE);
	static final AtomicIntegerArray vals = new AtomicIntegerArray(SIZE);
	static volatile byte[] data;

	static final Pattern namePattern = Pattern.compile("([A-Z][A-Z][A-Z]+|ST [A-Z][A-Z][A-Z]+)");

	static volatile int available;

	static final ArrayBlockingQueue<Long> byteArrayQueue = new ArrayBlockingQueue<Long>(1024, true);

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

    		for (Thread t : threads) {
	    		t.start();
    		}

    		int a = 0;
    		int i = 0;
    		int j = 0;
    		for (int c = 32 * 1024 * 1024; (c = parkingTicketsStream.read(data, a, c)) > 0; ) {
    			a += c;
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

    		for (int t = 0; t < n; t++) {
    			try {
					byteArrayQueue.put(0L);
				}
    			catch (InterruptedException e) {
					e.printStackTrace();
				}
    		}

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
		String threadName = Thread.currentThread().getName();
		Matcher nameMatcher = namePattern.matcher("");

		// local access faster than volatile fields
		byte[] data = ParkingTicketsStats.data;

		for (;;) {
			Long block_start_end = null;
			do {
				try {
					block_start_end = byteArrayQueue.poll(5, TimeUnit.MILLISECONDS);
				}
				catch (InterruptedException e) {
					e.printStackTrace();
					continue;
				}
			} while (block_start_end == null);

			if (block_start_end == 0) {
				break;
			}
			final int block_start = (int) (block_start_end >>> 32);
			final int block_end = (int) (long)block_start_end;

			// process block as fields
			// save fields 4 (set_fine_amount) and 7 (location2)
			int start = block_start;
			int column = 0;
			int fine = 0;
			String location = null;
			// process block
			while (start < block_end) {
				int end = start;
				while (end < block_end && data[end] != ',' && data[end] != '\n') { end++; }

				if (column == 4) {
		    		final String set_fine_amount = new String(data, start, end - start);
		    		try {
			    		fine = Integer.parseInt(set_fine_amount);
		    		}
		    		catch (final NumberFormatException e) {
		    			System.out.print(e.getClass().getSimpleName() +": "+ set_fine_amount);
		    		}
				}
				else if (column == 7) {
					if (fine > 0) {
						location = new String(data, start, end - start);

			    		nameMatcher.reset(location);
			    		if (nameMatcher.find()) {
			    			final String name = nameMatcher.group();
			    			add(name, fine);
		    			}
					}
				}

				column++;
				if (end < block_end && data[end] == '\n') {
					fine = 0;
					location = null;
					column = 0;
				}
				start = end + 1;
			}
		}

		println(System.currentTimeMillis(), "Thread ["+ threadName +"] ending normally");
    }

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

		keys.compareAndSet(i, null, k);
//		String k0 = keys.getAndSet(i, k);
//		if (k0 != null && !k0.equals(k)) {
//			println("Key hash clash: first "+ k0 +" and "+ k);
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