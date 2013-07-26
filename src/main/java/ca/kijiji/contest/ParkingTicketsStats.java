package ca.kijiji.contest;

import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.procedure.TObjectIntProcedure;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Comparator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Pipeline decomposition:
 * - InputStream
 * - byte[] blocks
 * - lines
 * - fields (e.g. fine, location)
 * - parse fine, location
 * - hash location
 *
 */
public class ParkingTicketsStats {

	// 24-bit indices (16M possible entries)
	static final int BITS = 24;
	static final int UNUSED_BITS = 32 - BITS;
	static final int SIZE = 1 << BITS;
	static final int MASK = SIZE - 1;

	static volatile byte[] data;

	static final Pattern namePattern = Pattern.compile("([A-Z][A-Z][A-Z]+|ST [A-Z][A-Z][A-Z]+)");

    static final int nWorkers = 4;
	static final TObjectIntHashMap<String>[] maps = new TObjectIntHashMap[nWorkers];

    public static SortedMap<String, Integer> sortStreetsByProfitability(final InputStream parkingTicketsStream) {
    	printInterval("Pre-entry initialization");
    	//printProperty("os.arch");

    	try {
			final int available = parkingTicketsStream.available();
    		println(System.currentTimeMillis(), "Bytes available: "+ available);

    		data = new byte[available];
    		final byte[] data = ParkingTicketsStats.data;

	        final ArrayBlockingQueue<Long> queues[] = new ArrayBlockingQueue[nWorkers];
	        final Worker workers[] = new Worker[nWorkers];

	        for (int t = 0; t < nWorkers; t++) {
	        	queues[t] = new ArrayBlockingQueue<>(256);
	        	maps[t] = new TObjectIntHashMap(15000);
	        	workers[t] = new Worker(queues[t], maps[t]);
	        	workers[t].start();
	        }

    		int a = 0;
    		int i = 0;
    		int j = 0;
    		int t = 0;
    		for (int c = 16 * 1024 * 1024; (c = parkingTicketsStream.read(data, a, c)) > 0; ) {
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

				while (data[--j] != '\n') {}

				final long ij = (long)i << 32 | (long)j & 0x00ffffffff;
				try {
					queues[t].put(ij);
				}
				catch (final InterruptedException e) {
					e.printStackTrace();
				}
    			t = (++t) % nWorkers;

    			if (available - a < c) {
    				c = available - a;
    			}
    		}

	    	printInterval("Local initialization: read remaining of "+ a +" total bytes");

	        for (t = 0; t < nWorkers; t++) {
				try {
					queues[t].put(0L);
				}
				catch (final InterruptedException e) {
					e.printStackTrace();
				}
	        }

	        for (t = 0; t < nWorkers; t++) {
		        try {
					workers[t].join();
				}
		        catch (final InterruptedException e) {
					e.printStackTrace();
				}
	        }

	    	printInterval("All worker threads completed");
    	}
    	catch (final IOException e) {
			e.printStackTrace();
		}

    	final SortedMap<String, Integer> sorted = new MergeMap();

    	printInterval("Maps merged");

        return sorted;
    }

	public static int hash(final String k) {
		int h = 0;
		try {
			for (final byte b : k.getBytes("UTF-8")) {
				final int c = (b == ' ') ? 0 : (int)b & 0x00FF - 64;
				h = h * 71 + c;
				h = (h ^ (h >>> BITS)) & MASK;
			}
		}
		catch (final UnsupportedEncodingException e) {}

		return h;
	}

    static volatile long lastTime = System.currentTimeMillis();

    public static void printInterval(final String name) {
    	final long time = System.currentTimeMillis();
    	println(time, name +": "+ (time - lastTime) +" ms");
    	lastTime = time;
    }

    public static void printElement(final String key, final Map<String, Integer> streets) {
    	println(key +": $"+ streets.get(key));
    }

    public static void printProperty(final String name) {
		println(name +": "+ System.getProperty(name));
    }

    public static void println(final long time, final String line) {
    	println(time%10000 +" "+ line);
    }

    public static void println(final String line) {
    	System.out.println(line);
    }

    public final static class Worker extends Thread {
		private final ArrayBlockingQueue<Long> queue;
		private final TObjectIntHashMap<String> map;
		private final Matcher nameMatcher = namePattern.matcher("");

		public Worker(final ArrayBlockingQueue<Long> queue, final TObjectIntHashMap<String> map) {
			this.queue = queue;
			this.map = map;
		}

        public final void run() {
			// local access faster than volatile fields
			final byte[] data = ParkingTicketsStats.data;

			for (;;) {
				final long ij;
				try {
					ij = queue.poll(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
				}
				catch (final InterruptedException e) {
					e.printStackTrace();
					continue;
				}

				if (ij == 0) {
					break;
				}

				int i = (int)(ij >>> 32);
    			final int j = (int)ij;

				// process block
				for (int m; i < j; i = m) {
					// process a line
					m = i;
					while (m < j && data[m++] != (byte)'\n') {}

					// split out fields 4 (set_fine_amount) and 7 (location2)
		    		String set_fine_amount = "0";
		    		String location2 = "";

					int k;
					int c = 0;
					do {
						k = i;
						while (k < m && data[k] != ',' && data[k] != '\n') { k++; }
						if (c == 4) {
							set_fine_amount = new String(data, i, k - i);
						}
						else if (c == 7) {
				    		location2 = new String(data, i, k - i);
						}
						c++;
						i = k + 1;
					} while (i < m);

	    			// parse fine
		    		int amount = 0;
		    		try {
			    		amount = Integer.parseInt(set_fine_amount);
		    		}
		    		catch (final NumberFormatException e) {
		    			System.out.print(e.getClass().getSimpleName() +": "+ set_fine_amount);
		    		}

		    		// parse location2
		    		nameMatcher.reset(location2);
		    		if (nameMatcher.find()) {
		    			final String name = nameMatcher.group();
		    			map.adjustOrPutValue(name, amount, amount);
					}
					else {
						// name could not be parsed, print out select subset of these errors
						if (location2.indexOf("KING") >= 0 && location2.indexOf("PARKING") == -1) {
		    				println(""+ location2);
						}
					}
				}
			}
        }
    }

	@SuppressWarnings("serial")
	public static final class MergeMap extends TreeMap<String, Integer> {
		public MergeMap() {
			super(new Comparator<String>() {
				public int compare(final String o1, final String o2) {
					final int c = getMerged(o2) - getMerged(o1);
					if (c != 0) return c;
					return o2.compareTo(o1);
				}
			});

			for (final TObjectIntHashMap<String> map : maps) {
				map.forEachEntry(new TObjectIntProcedure<String>() {
					public boolean execute(final String k, final int v) {
						final Integer i = get(k);
						if (i == null) {
							put(k, v);
						} else {
							put(k,  i+v);
						}
						return true;
					}});
			}
		}

		private static Integer getMerged(final Object key) {
			int v = 0;
			for (final TObjectIntHashMap<String> map : maps) {
				v += map.get(key);
			}
			return v;
		}
	}
}