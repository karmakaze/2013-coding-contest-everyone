package ca.kijiji.contest;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Map;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class OpenStringIntHashMap {
	public volatile long pad7, pad6, pad5, pad4, pad3, pad2, pad1;

	public final int NO_ELEMENT_VALUE = 0;

	private final int capacity;
	private final String[] keys;
	private final long[] hashAndValues; // each long is value << 32 | hash

	private final HashFunction murmur3 = Hashing.murmur3_32();

	public volatile long Pad1, Pad2, Pad3, Pad4, Pad5, Pad6, Pad7;

	public OpenStringIntHashMap(int capacity) {
		this.capacity = capacity;
		keys = new String[capacity];
		hashAndValues = new long[capacity];
		pad7 = pad6 = pad5 = pad4 = pad3 = pad2 = pad1 = 7;
		Pad1 = Pad2 = Pad3 = Pad4 = Pad5 = Pad6 = Pad7 = 7;
	}

	public void clear() {
		synchronized (keys) {
			Arrays.fill(keys, 0);
			Arrays.fill(hashAndValues, 0);
		}
	}

	public int get(String key) {
		int hash = hash(key);
		int cur = hash % capacity;
		if (cur < 0) cur += capacity;

		int v = scanValueHash(hash, cur, capacity);
		if (v == NO_ELEMENT_VALUE) {
			v = scanValueHash(hash, 0, cur);
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
	private int scanValueHash(int hash, int cur, int end) {
		for (; cur < end; cur++) {
			long vh = hashAndValues[cur];
			int h = (int) vh;
			if (h  == hash) {
				synchronized (keys) {
					return (int) (hashAndValues[cur] >>> 32);
				}
			}
			else if (h == 0) {
				synchronized (keys) {
					do {
						vh = hashAndValues[cur];
						h = (int) vh;
						if (h  == hash) {
							return (int) (vh >>> 32);
						}
					} while (h != 0 && ++cur < end);
				}
				return NO_ELEMENT_VALUE;
			}
		}
		return NO_ELEMENT_VALUE;
	}

	private boolean put(String key, int hash, int value, int cur, int end) {
		for (; cur < end; cur++) {
			long vh = hashAndValues[cur];
			int h = (int) vh;
			if (h == hash) {
				synchronized (keys) {
					hashAndValues[cur] = (long)value << 32 | (long)h & 0x00FFFFFFFF;
				}
				return true;
			}
			else if (h == 0) {
				synchronized (keys) {
					do {
						vh = hashAndValues[cur];
						h = (int) vh;
						if (h == hash) {
							hashAndValues[cur] = (long)value << 32 | (long)h & 0x00FFFFFFFF;
							return true;
						}
						else if (h == 0) {
							hashAndValues[cur] = (long)value << 32 | (long)h & 0x00FFFFFFFF;
							keys[cur] = key;
							return true;
						}
					} while (++cur < end);
				}
				return false;
			}
		}
		return false;
	}

	private boolean adjustOrPutValue(String key, int hash, int value, int cur, int end) {
		for (; cur < end; cur++) {
			long vh = hashAndValues[cur];
			int h = (int) vh;
			if (h == hash) {
				synchronized (keys) {
					hashAndValues[cur] += (long)value << 32;
				}
				return true;
			}
			else if (h == 0) {
				synchronized (keys) {
					do {
						vh = hashAndValues[cur];
						h = (int) vh;
						if (h == hash) {
							hashAndValues[cur] += (long)value << 32;
							return true;
						} else if (h == 0) {
							hashAndValues[cur] = (long)value << 32 | (long)h & 0x00FFFFFFFF;
							keys[cur] = key;
							return true;
						}
					} while (++cur < end);
				}
				return false;
			}
		}
		return false;
	}

	public void putAllTo(Map<String, Integer> dest) {
		putRangeTo(0, capacity, dest);
	}

	protected void putRangeTo(int cur, int end, Map<String, Integer> dest) {
		synchronized (keys) {
			String key;
			for (; cur < end; cur++) {
				if ((key = keys[cur]) != null) {
					synchronized (dest) {
						dest.put(key, (int) (hashAndValues[cur] >> 32));
					}
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
