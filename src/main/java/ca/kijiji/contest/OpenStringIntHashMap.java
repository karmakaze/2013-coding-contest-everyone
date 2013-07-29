package ca.kijiji.contest;

import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicIntegerArray;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class OpenStringIntHashMap {
	public volatile long pad7, pad6, pad5, pad4, pad3, pad2, pad1;

	public final int NO_ELEMENT_VALUE = 0;

	private final int capacity;
	private final int[] hashes;
	private final String[] keys;
	private final AtomicIntegerArray values;

	private final HashFunction murmur3 = Hashing.murmur3_32();

	public volatile long Pad1, Pad2, Pad3, Pad4, Pad5, Pad6, Pad7;

	public OpenStringIntHashMap(int capacity) {
		this.capacity = capacity;
		hashes = new int[capacity];
		keys = new String[capacity];
		values = new AtomicIntegerArray(capacity);
		pad7 = pad6 = pad5 = pad4 = pad3 = pad2 = pad1 = 7;
		Pad1 = Pad2 = Pad3 = Pad4 = Pad5 = Pad6 = Pad7 = 7;
	}

	public int get(String key) {
		int hash = hash(key);
		int i0 = hash % capacity;
		if (i0 < 0) i0 += capacity;

		return get(hash, i0, true);
	}

	public void put(String key, int value) {
		int hash = hash(key);
		int i0 = hash % capacity;
		if (i0 < 0) i0 += capacity;

		put(key, hash, value, i0, true);
	}

	public void adjustOrPutValue(String key, int value) {
		int hash = hash(key);
		int i0 = hash % capacity;
		if (i0 < 0) i0 += capacity;

		adjustOrPutValue(key, hash, value, i0, true);
	}

	public void putAllTo(Map<String, Integer> dest) {
		synchronized (hashes) {
			int i = 0;
			for (String key : keys) {
				if (key != null) {
					dest.put(key, values.get(i));
				}
				i++;
			}
		}
	}

	private int hash(String key) {
		try {
			int h = murmur3.hashBytes(key.replace(" ", "").getBytes("UTF-8")).asInt();
			if (h == 0) h = capacity;
			return h;
		}
		catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			return 0;
		}
	}

	private int get(int hash, int i0, boolean recurse) {
		for (int i = i0; i < capacity; i++) {
			int hi = hashes[i];
			if (hash == hi) {
				return values.get(i);
			}
			if (hi == 0) {
				if (recurse) {
					synchronized (hashes) {
						return get(hash, i0, false);
					}
				}
				return NO_ELEMENT_VALUE;
			}
		}
		for (int i = 0; i < i0; i++) {
			int hi = hashes[i];
			if (hash == hi) {
				return values.get(i);
			}
			if (hi == 0) {
				if (recurse) {
					synchronized (hashes) {
						return get(hash, i0, false);
					}
				}
				return NO_ELEMENT_VALUE;
			}
		}
		return NO_ELEMENT_VALUE;
	}

	public void put(String key, int hash, int value, int i0, boolean recurse) {
		for (int i = i0; i < capacity; i++) {
			int hi = hashes[i];
			if (hash == hi) {
				values.set(i, value);
				return;
			}
			if (hi == 0) {
				if (recurse) {
					synchronized (hashes) {
						put(key, hash, value, i, false);
					}
				}
				else {
					hashes[i] = hash;
					keys[i] = key;
					values.set(i, value);
				}
				return;
			}
		}
		for (int i = 0; i < i0; i++) {
			int hi = hashes[i];
			if (hash == hi) {
				values.set(i, value);
				return;
			}
			if (hi == 0) {
				if (recurse) {
					synchronized (hashes) {
						put(key, hash, value, i, false);
					}
				}
				else {
					hashes[i] = hash;
					keys[i] = key;
					values.set(i, value);
				}
				return;
			}
		}
		throw new IllegalStateException("Exceeded capacity "+ capacity);
	}

	private void adjustOrPutValue(String key, int hash, int value, int i0, boolean recurse) {
		for (int i = i0; i < capacity; i++) {
			int hi = hashes[i];
			if (hash == hi) {
				values.addAndGet(i, value);
				return;
			}
			if (hi == 0) {
				if (recurse) {
					synchronized (hashes) {
						put(key, hash, value, i, false);
					}
				}
				else {
					hashes[i] = hash;
					keys[i] = key;
					values.addAndGet(i, value);
				}
				return;
			}
		}
		for (int i = 0; i < i0; i++) {
			int hi = hashes[i];
			if (hash == hi) {
				values.set(i, value);
				return;
			}
			if (hi == 0) {
				if (recurse) {
					synchronized (hashes) {
						put(key, hash, value, i, false);
					}
				}
				else {
					hashes[i] = hash;
					keys[i] = key;
					values.addAndGet(i, value);
				}
				return;
			}
		}
		throw new IllegalStateException("Exceeded capacity "+ capacity);
	}
}
