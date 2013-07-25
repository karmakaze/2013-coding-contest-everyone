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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

public class ParkingTicketsStats extends Thread {

	// 24-bit indices (16M possible entries)
	static final int BITS = 24;
	static final int UNUSED_BITS = 32 - BITS;
	static final int SIZE = 1 << BITS;
	static final int MASK = SIZE - 1;
	static final AtomicReferenceArray<String> keys = new AtomicReferenceArray<String>(SIZE);
	static final AtomicIntegerArray vals = new AtomicIntegerArray(SIZE);
	static volatile byte[] data;

	static final String name = "([A-Z][A-Z][A-Z]+|ST [A-Z][A-Z][A-Z]+)";
	static final Pattern namePattern = Pattern.compile(name);

	static volatile int available;

	static final int nThreads = 10;
	@SuppressWarnings("unchecked")
	static final ArrayBlockingQueue<Long>[] byteArrayQueues = new ArrayBlockingQueue[nThreads];

	final ArrayBlockingQueue<Long> byteArrayReadQueue;

    public static SortedMap<String, Integer> sortStreetsByProfitability(final InputStream parkingTicketsStream) {
    	printInterval("Pre-entry initialization");
    	//printProperty("os.arch");

    	try {
			available = parkingTicketsStream.available();
    		println(System.currentTimeMillis(), "Bytes available: "+ available);

	        final ExecutorService exec = Executors.newCachedThreadPool();

	        // Preallocate RingBuffer with 1024 ValueEvents
	        final Disruptor<ValueEvent> disruptor = new Disruptor<ValueEvent>(ValueEvent.EVENT_FACTORY, 1024, exec);

	        // Build dependency graph
	        final ValueEventHandler handler = new ValueEventHandler();
	        disruptor.handleEventsWith(handler);

	        final RingBuffer<ValueEvent> ringBuffer = disruptor.start();

    		data = new byte[available];

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

    			final long ij = (long)i << 32 | (long)j & 0x0ffffffffL;
	            // Two phase commit. Grab one of the slots
	            final long seq = ringBuffer.next();
	            final ValueEvent valueEvent = ringBuffer.get(seq);
	            valueEvent.setValue(ij);
	            ringBuffer.publish(seq);

    			if (available - a < c) {
    				c = available - a;
    			}
    		}

	    	printInterval("Local initialization: read remaining of "+ a +" total bytes");

	        disruptor.shutdown();
	        exec.shutdown();

	    	printInterval("All worker threads completed");
    	}
    	catch (final IOException e) {
			e.printStackTrace();
		}

//    	println("Size: "+ streets.size());

    	final SortedMap<String, Integer> sorted = new TreeMap<String, Integer>(new Comparator<String>() {
			public int compare(final String o1, final String o2) {
				final int c = get(o2) - get(o1);
				if (c != 0) return c;
				return o2.compareTo(o1);
			}});

    	final int B = SIZE / 2;
//    	final int C = B + B + 1;

    	final Thread t0 = new Thread(null, null, "g0", 1024) {
    		public void run() {
    	    	for (int i = 0; i < B; i++) {
    	    		final int v = vals.get(i);
    	    		if (v != 0) {
    	    			synchronized (sorted) {
    		    			sorted.put(keys.get(i), v);
    	    			}
    	    		}
    	    	}
    		}
    	};
    	t0.start();

    	final Thread t1 = new Thread(null, null, "g1", 1024) {
    		public void run() {
    	    	for (int i = B; i < SIZE; i++) {
    	    		final int v = vals.get(i);
    	    		if (v != 0) {
    	    			synchronized (sorted) {
    		    			sorted.put(keys.get(i), v);
    	    			}
    	    		}
    	    	}
    		}
    	};
    	t1.start();

    	try { t0.join(); } catch (final InterruptedException e) {}
    	try { t1.join(); } catch (final InterruptedException e) {}

    	printInterval("Populated TreeSet");

        return sorted;
    }

    ParkingTicketsStats(final int id, final ArrayBlockingQueue<Long> byteArrayReadQueue) {
    	this.byteArrayReadQueue = byteArrayReadQueue;
    }

    public void run() {
    	worker(byteArrayReadQueue);
    }

    /**
     * worker parallel worker takes blocks of bytes read and processes them
     */
    static final void worker(final ArrayBlockingQueue<Long> byteArrayReadQueue) {
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

	public static void add(final String k, final int d) {
		final int i = hash(k);
		vals.addAndGet(i, d);

//		String k0 = keys[i];
//		if (k0 != null && !k0.equals(k)) {
//			println("Key hash clash: first "+ k0 +" and "+ k);
//		}
//		else {
			keys.set(i, k);
//		}
	}
	public static int get(final String k) {
		final int i = hash(k);
		return vals.get(i);
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

    public final static class ValueEventHandler implements EventHandler<ValueEvent> {
        public void onEvent(final ValueEvent event, final long sequence, final boolean endOfBatch) throws Exception {
            System.out.println("Sequence: " + sequence);
            System.out.println("ValueEvent: " + event.getValue());
		//	String threadName = Thread.currentThread().getName();
			final Matcher nameMatcher = namePattern.matcher("");

			// local access faster than volatile fields
			final byte[] data = ParkingTicketsStats.data;

			final ArrayList<String> parts = new ArrayList<>();

			final long ij = event.value;

			int i = (int) (ij >>> 32);
			final int j = (int) (long) ij;
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
					//String tag_number_masked = parts[0];
					//String date_of_infraction = parts[1];
					//String infraction_code = parts[2];
					//String infraction_description = parts[3];
		    		final String sfa = parts.get(4);
		    		Integer set_fine_amount = 0;
		    		try {
			    		set_fine_amount = Integer.parseInt(sfa);
		    		}
		    		catch (final NumberFormatException e) {
		    			System.out.print(e.getClass().getSimpleName() +": "+ sfa);
		    		}
					//String time_of_infraction = parts[5];
					//String location1 = parts[6];
		    		final String location2 = parts.get(7);
					//String location3 = parts[8];
					//String location4 = parts[9];
		    		//String province = parts[10];
		    		nameMatcher.reset(location2);
		    		if (nameMatcher.find()) {
		    			final String l = nameMatcher.group();
		    			add(l, set_fine_amount);
	    			}
	    			else {
	    				if (location2.indexOf("KING") >= 0 && location2.indexOf("PARKING") == -1) {
		    				println(""+ location2);
	    				}
	    			}
	    		}
	    		catch (final ArrayIndexOutOfBoundsException e) {
	    			println(e.getClass().getSimpleName() +": "+ parts);
	    			e.printStackTrace();
	    		}
			}
        }
    }

    /**
     * WARNING: This is a mutable object which will be recycled by the RingBuffer.
     * You must take a copy of data it holds before the framework recycles it.
     */
    public final static class ValueEvent {
        private long value;

        public final long getValue() {
            return value;
        }

        public final void setValue(final long value) {
            this.value = value;
        }

        public final static EventFactory<ValueEvent> EVENT_FACTORY = new EventFactory<ValueEvent>() {
            public ValueEvent newInstance() {
                return new ValueEvent();
            }
        };
    }
}