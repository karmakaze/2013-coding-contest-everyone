package ca.kijiji.contest;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicIntegerArray;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class OpenStringIntHashMap {
	public volatile long pad7, pad6, pad5, pad4, pad3, pad2, pad1;

	public final int NO_ELEMENT_VALUE = 0;

	private final int capacity;
	private final String[] keys;
	private final AtomicIntegerArray hashAndValues;

	private final HashFunction murmur3 = Hashing.murmur3_32();

	public volatile long Pad1, Pad2, Pad3, Pad4, Pad5, Pad6, Pad7;

	public OpenStringIntHashMap(int capacity) {
		this.capacity = capacity;
		keys = new String[capacity];
		hashAndValues = new AtomicIntegerArray(2*capacity);
		pad7 = pad6 = pad5 = pad4 = pad3 = pad2 = pad1 = 7;
		Pad1 = Pad2 = Pad3 = Pad4 = Pad5 = Pad6 = Pad7 = 7;
	}

	public void clear() {
		Arrays.fill(keys, 0);
		int n2 = 2 * keys.length;
		for (int i = 0; i < n2; i++) {
			hashAndValues.set(i, 0);
		}
	}

	public int get(String key) {
		int hash = hash(key);
		int i0 = hash % capacity;
		if (i0 < 0) i0 += capacity;

		final int i2 = i0 * 2;

		for (int i = i2; i < capacity * 2; i += 2) {
			int h = hashAndValues.get(i);
			if (h == hash) {
				return hashAndValues.get(i + 1);
			}
			else if (h == 0) {
				return NO_ELEMENT_VALUE;
			}
		}
		for (int i = 0; i < i2; i += 2) {
			int h = hashAndValues.get(i);
			if (h == hash) {
				return hashAndValues.get(i + 1);
			}
			else if (h == 0) {
				return NO_ELEMENT_VALUE;
			}
		}
		return NO_ELEMENT_VALUE;
	}

	public void put(String key, int value) {
		int hash = hash(key);
		int i0 = hash % capacity;
		if (i0 < 0) i0 += capacity;

		final int i2 = i0 * 2;

		for (int i = i2; i < capacity * 2; i += 2) {
			if (hashAndValues.get(i) == hash) {
				hashAndValues.set(i + 1, value);
				return;
			}
			else if (hashAndValues.compareAndSet(i, 0, hash)) {
				hashAndValues.set(i + 1, value);
				synchronized (keys) {
					keys[i/2] = key;
				}
				return;
			}
		}
		for (int i = 0; i < i2; i += 2) {
			if (hashAndValues.get(i) == hash) {
				hashAndValues.set(i + 1, value);
				return;
			}
			else if (hashAndValues.compareAndSet(i, 0, hash)) {
				hashAndValues.set(i + 1, value);
				synchronized (keys) {
					keys[i/2] = key;
				}
				return;
			}
		}
		throw new IllegalStateException("Exceeded capacity "+ capacity);
	}

	public void adjustOrPutValue(String key, int value) {
		int hash = hash(key);
		int i0 = hash % capacity;
		if (i0 < 0) i0 += capacity;

		final int i2 = i0 * 2;

		for (int i = i2; i < capacity * 2; i += 2) {
			if (hashAndValues.get(i) == hash) {
				hashAndValues.addAndGet(i + 1, value);
				return;
			}
			else if (hashAndValues.compareAndSet(i, 0, hash)) {
				hashAndValues.addAndGet(i + 1, value);
				synchronized (keys) {
					keys[i/2] = key;
				}
				return;
			}
		}
		for (int i = 0; i < i2; i += 2) {
			if (hashAndValues.get(i) == hash) {
				hashAndValues.addAndGet(i + 1, value);
				return;
			}
			else if (hashAndValues.compareAndSet(i, 0, hash)) {
				hashAndValues.addAndGet(i + 1, value);
				synchronized (keys) {
					keys[i/2] = key;
				}
				return;
			}
		}
		throw new IllegalStateException("Exceeded capacity "+ capacity);
	}

	public void putAllTo(Map<String, Integer> dest) {
		synchronized (keys) {
			int i = 0;
			for (String key : keys) {
				if (key != null) {
					dest.put(key, hashAndValues.get(i*2 + 1));
				}
				i++;
			}
		}
	}

	protected void putRangeTo(int start, int end, Map<String, Integer> dest) {
		for (int i = start; i < end; i++) {
			String key = keys[i];
			if (key != null) {
				synchronized (dest) {
					dest.put(key, hashAndValues.get(i*2 + 1));
				}
			}
		}
	}

	private int hash(String key) {
		try {
			int h = murmur3.hashBytes(key.getBytes("UTF-8")).asInt();
			if (h == 0) h = capacity;
			return h;
		}
		catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			return 0;
		}
	}
}
