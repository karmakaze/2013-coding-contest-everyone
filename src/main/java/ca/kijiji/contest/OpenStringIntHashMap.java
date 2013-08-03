package ca.kijiji.contest;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicIntegerArray;

public class OpenStringIntHashMap {
	public volatile long pad7, pad6, pad5, pad4, pad3, pad2, pad1;

	public final int NO_ELEMENT_VALUE = 0;

	private final int capacity;
	private final String[] keys;
	private final int[] hashes;
	private final AtomicIntegerArray values;

	public volatile long Pad1, Pad2, Pad3, Pad4, Pad5, Pad6, Pad7;

	public OpenStringIntHashMap(int capacity) {
		this.capacity = capacity;
		keys = new String[capacity];
		hashes = new int[capacity];
		values = new AtomicIntegerArray(capacity);
		pad7 = pad6 = pad5 = pad4 = pad3 = pad2 = pad1 = 7;
		Pad1 = Pad2 = Pad3 = Pad4 = Pad5 = Pad6 = Pad7 = 7;
	}

	public void clear() {
		synchronized (this) {
			Arrays.fill(keys, 0);
			Arrays.fill(hashes, 0);
		}
		for (int i = 0; i < capacity; i++) values.set(0,  0);
	}

	public int get(String key) {
		int hash = hash(key);
		int cur = hash % capacity;
		if (cur < 0) cur += capacity;

		int v = scanValueHash(key, hash, cur, capacity);
		if (v == NO_ELEMENT_VALUE) {
			v = scanValueHash(key, hash, 0, cur);
		}
		return v;
	}

	public void put(String key, int value) {
		int hash = hash(key);
		int cur = hash % capacity;
		if (cur < 0) cur += capacity;

		if (!put(key, hash, value, cur, capacity)) {
			if (!put(key, hash, value, 0, cur)) {
				throw new IllegalStateException("Exceeded capacity "+ capacity);
			}
		}
	}

	public void adjustOrPutValue(String key, int value) {
		int hash = hash(key);
		int cur = hash % capacity;
		if (cur < 0) cur += capacity;

		if (!adjustOrPutValue(key, hash, value, cur, capacity)) {
			if (!adjustOrPutValue(key, hash, value, 0, cur)) {
				throw new IllegalStateException("Exceeded capacity "+ capacity);
			}
		}
	}

	/**
	 * @param hash
	 * @param cur
	 * @param end
	 * @return the found value with hash, otherwise NO_ELEMENT_VALUE
	 */
	private int scanValueHash(String key, int hash, int cur, int end) {
		for (; cur < end; cur++) {
			int h = hashes[cur];
			if (h == hash) {
				return values.get(cur);
			}
			else if (h == 0) {
				synchronized (this) {
					do {
						h = hashes[cur];
						if (h == hash) {
							return values.get(cur);
						}
					} while (h != 0 && ++cur < end);

					return NO_ELEMENT_VALUE;
				}
			}
		}
		return NO_ELEMENT_VALUE;
	}

	private boolean put(String key, int hash, int value, int cur, int end) {
		for (; cur < end; cur++) {
			int h = hashes[cur];
			if (h == hash) {
				synchronized (this) {
					values.set(cur,  value);
					return true;
				}
			}
			else if (h == 0) {
				synchronized (this) {
					do {
						h = hashes[cur];
						if (h == hash) {
							values.set(cur, value);
							return true;
						}
						else if (h == 0) {
							values.set(cur, value);
							hashes[cur] = hash;
							keys[cur] = key;
							return true;
						}
					} while (++cur < end);

					return false;
				}
			}
		}
		return false;
	}

	private final boolean adjustOrPutValue(String key, int hash, int value, int cur, int end) {
		for (; cur < end; cur++) {
			int h = hashes[cur];
			if (h == hash) {
				values.getAndAdd(cur, value);
				return true;
			}
			else if (h == 0) {
				synchronized (this) {
					do {
						h = hashes[cur];
						if (h == hash) {
							values.getAndAdd(cur, value);
							return true;
						} else if (h == 0) {
							values.getAndAdd(cur, value);
							hashes[cur] = hash;
							keys[cur] = key;
							return true;
						}
					} while (++cur < end);

					return false;
				}
			}
		}
		return false;
	}

	public void putAllTo(Map<String, Integer> dest) {
		putRangeTo(0, capacity, dest);
	}

	protected void putRangeTo(int cur, int end, Map<String, Integer> dest) {
		synchronized (this) {
			for (String key; cur < end; cur++) {
				if ((key = keys[cur]) != null) {
					synchronized (dest) {
						dest.put(key, values.get(cur));
					}
				}
			}
		}
	}

	private int hash(String key) {
		int hash = 0;

		int l = key.length();
		for (int i = 0; i < l; i++) {
            hash = (16777619 * hash) ^ key.charAt(i);
 		}
		return hash;
	}
}
