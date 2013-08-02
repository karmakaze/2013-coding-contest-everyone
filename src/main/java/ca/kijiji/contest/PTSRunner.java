package ca.kijiji.contest;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PTSRunner extends Thread {
    private byte[] input;
    private int offset;
    private int len;

    private Object lock;
    private PackingStreetMap packedData;
    private boolean packed = false;
    
    // I don't use a PackingStreetMap here because it has a different semantic that
    // the one needed here. Namely, the PackingStreetMap will duplicate objects, which
    // is not needed here
    //private Map<String, Street> parsedData = new HashMap<>();
    private Map<String, Street> parsedData = new HashMap<>();
    private StreetNames streetNames;
//    private static final Logger LOG = LoggerFactory.getLogger(PTSRunner.class);

    public PTSRunner(byte[] buf, int offset, int len, StreetNames streetNames, PackingStreetMap packedData, Object lock) {
	this.input = buf;
	this.offset = offset;
	this.len = len;
	this.streetNames = streetNames;
	this.lock = lock;
	this.packedData = packedData;
    }

    @Override
    public void run() {
	process();
//	long start = System.currentTimeMillis();
	matchToClean();
	//LOG.info(String.format("Thread %d completed matching in %d ms", 0, (System.currentTimeMillis() - start)));
	// start = System.currentTimeMillis();
	pack();
	// LOG.info(String.format("Thread completed in %d ms", (System.currentTimeMillis() - start)));

	synchronized (lock) {
	    lock.notifyAll();
	}
    }

    /**
     * Matches the extracted street names to the clean street names. 
     */
    private void matchToClean() {
	// I also tested packing this thread's data and then merging to the destination,
	// but it doesn't seem to make a big difference. In some cases it's even worse
	Iterator<Street> it = parsedData.values().iterator();
	while (it.hasNext()) {

	    Street s = it.next();
	    String matched = streetNames.getSimilar(s, ParkingTicketsStats.THRESHOLD);
	    if (matched == null) {
		// Unable to match this street name to an official street name, discard
		it.remove();
		continue;
	    } else {
		s.setName(matched);
	    }
	}
    }
    
    /**
     * Adds all parsed and matched data to the shared "packedData" map
     */
    private void pack() {
	packedData.addAll(parsedData.values());
	packed = true;
    }

    private void process() {
	ByteInputReader reader = new ByteInputReader();
	reader.input = input;
	reader.position = offset;
	reader.end = offset + len;
	while (!reader.eof()) {
	    Street street = reader.extract();
	    if (street == null) {
		continue;
	    }
	    
	    // Matching to clean street names could be done here,
	    // but it's slightly slower than matching done at the pack() step
	    Street v = parsedData.get(street.getName());
	    if (v != null) {
		v.addProfit(street);
	    } else {
		parsedData.put(street.getName(), street);
	    }
	}
    }
    
    public Collection<String> getParsedStreetNames() {
	return Collections.unmodifiableCollection(parsedData.keySet());
    }
    
    public boolean isPacked() {
	return packed;
    }
}