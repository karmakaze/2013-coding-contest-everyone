package ca.kijiji.contest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

/**
 * This class parses a stream containing clean street names into a hashmap: keys are the first three chars of the street
 * name, values are collections of all the streetnames that start with those three chars. <br/>
 * Keys are sorted in alphabetical order, value are unsorted. Values are kept in lists because they are meant to be
 * traversed. In particular, ArrayLists are the ones that seem to perform better both in creation and access
 * (unsurprisingly since the average list size is around 4)
 * 
 * @author Chiara
 */
public class StreetNames extends HashMap<String, Collection<String>> {

    private final static int STREET_PREFIX_LENGTH = 3;
    private static final long serialVersionUID = 3752793494180847256L;
    private static final String DISTANCE_LOCAL = "local";
    private static final String DISTANCE_DL = "dl";

    enum Distance {
	LOCAL, DL
    }

    private final Distance distance;

    private static DamerauLevenshteinAlgorithm dlAlgorithm = new DamerauLevenshteinAlgorithm();

    public StreetNames(String distanceFunction) {
	switch (distanceFunction) {
	case DISTANCE_DL:
	    distance = Distance.DL;
	    break;
	case DISTANCE_LOCAL:
	default:
	    distance = Distance.LOCAL;
	}
    }

    public StreetNames(Distance distance) {
	this.distance = distance;
    }

    public StreetNames copy() {
	StreetNames cp = new StreetNames(distance);
	for (String s : keySet()) {
	    Collection<String> myData = get(s);
	    Collection<String> data = new ArrayList<>(myData);
	    cp.put(s, data);
	}
	return cp;
    }

    /**
     * Parses the stream. Each line is expected to contain a single street name, in a consistent uppercase or lowercase
     * or camelcase format (i.e.: street names are not changed and keys are computed using the exact first three
     * letters, with no modifications). <br/>
     * The method adds to the current streetnames, so it could be invoked several times with different sources.
     * 
     * @param str
     *            the stream to parse. Opening and closing of the stream must be performed outside this method.
     * @throws IOException
     */
    public synchronized void parse(InputStream str) throws IOException {
	BufferedReader in = new BufferedReader(new InputStreamReader(str));

	while (true) {
	    String line = in.readLine();
	    if (line == null) {
		break;
	    }
	    while (line.length() < 3) {
		line += ' '; // Yes I know this is not efficient, but it happens very seldom
	    }
	    String key = line.substring(0, STREET_PREFIX_LENGTH);
	    Collection<String> streets = get(key);
	    if (streets == null) {
		streets = new ArrayList<>();
		put(key, streets);
	    }
	    streets.add(line);
	}
    }

    /**
     * 
     * In order to determine if an extracted street name matches a clean street name, the algorithm imposes that the
     * first three characters be equal in both strings, then applies a MEASURE OF SIMILARITY to the remainder of the
     * string. The match is the clean street name that - begins with the same three letters - has the maximum value of
     * similarity to the target string with respect to all of the other "clean" street names - has a similarity above a
     * THRESHOLD.
     * 
     * If no match can be found, the extracted street data is discarded. When all streets are matched and profits
     * summed, the result is merged by each thread to a common dictionary, that holds the unsorted street names and the
     * profits
     * 
     * The measure of similarity can be chosen among two: one. called "local", favours strings with higher
     * correspondance in the first characters, with a low penalization for dissimilarities in the last part of the
     * string. This accounts for the fact that most suffixes are not removed from the street names extracted from the
     * datafile, while it is expected that the first part of street names has a higher probability of being written
     * correctly (it's the same assumption that lead to impose that the first three chars must match). However,
     * insertions or deletions that happen early in the string are highly penalized. The second measure of similarity is
     * calculated using a standard distance metric, the DamerauLevenshtein, that allows for constant penalization of
     * insertions, deletions, substitutions and swaps.
     * 
     * Both measures seem to work fine, even considering that the clean street names do not contain the suffix and the
     * extracted ones do, so I prefer to use the local one because it's faster.
     * 
     * @param street
     * @param threshold
     * @return
     */
    public synchronized String getSimilar(Street street, double threshold) {
	Object key = street.getName().substring(0, STREET_PREFIX_LENGTH);
	Collection<String> streets = get(key);
	if (streets == null) {
	    return null;
	}

	double maxSim = 0;
	String mostSim = null;
	// Since this implementation imposes that the first STREET_PREFIX_LENGTH
	// be equal for a match to be found (or else similarity is 0) it's safe
	// to compute the distance only on the rest of the strings
	String target = street.getName().substring(STREET_PREFIX_LENGTH);
	for (String s : streets) {
	    double sim;
	    switch (distance) {
	    case DL:
		sim = dlSimilarity(s.substring(STREET_PREFIX_LENGTH), target);
	    case LOCAL: // Also default value
	    default:
		sim = similarity(s.substring(STREET_PREFIX_LENGTH), target);
		break;
	    }
	    if (sim > maxSim) {
		maxSim = sim;
		mostSim = s;
	    }
	}
	if (maxSim >= threshold) {
	    // System.out.println(street.getName()+"\t"+mostSim);
	    return mostSim;
	} else {
	    return null;
	}
    }

    /**
     * Measure of similarity that uses the Damerau - Levenshtein distance
     * @param ref
     * @param street
     * @return
     */
    public final static double dlSimilarity(String ref, String street) {
	double distance = dlAlgorithm.execute(ref, street);
	return 1 - (distance / Math.max(ref.length(), street.length()) + STREET_PREFIX_LENGTH);
    }

    
    /**
     * "Local" measure of similarity. Pretty heuristic, à-la "it's fine as long as it works".
     * Positive weight of each matching charachter decreases exponentially with character index. 
     * After second word (second whitespace) weight is further decreased. 
     * Whitespaces are not considered in the mathing (PARKA VE == PARK AVE) 
     * @param ref
     * @param street
     * @return
     */
    public final static double similarity(String ref, String street) {

	int ai = 0;
	int bi = 0;
	int al = ref.length();
	int bl = street.length();
	if (al > bl) {
	    return 0;
	}
	double s = 0;
	double div = 0;
	double wordWeight = 1;
	boolean skipWord = false;
	int words = 1;
	while (ai < al && bi < bl) {
	    double wa = ((double) ai) / al;
	    double wb = ((double) bi) / bl;
	    if (ref.charAt(ai) == ' ') {
		ai++;
		skipWord = true;
		continue;
	    }
	    if (street.charAt(bi) == ' ') {
		bi++;
		skipWord = true;
		continue;
	    }
	    if (skipWord) {
		skipWord = false;
		words++;
		wordWeight = words > 2 ? wordWeight / 2 : wordWeight;
	    }
	    double w = Math.exp(-Math.max(wa, wb)) * wordWeight;
	    if (ref.charAt(ai) == street.charAt(bi)) {
		s += w;
	    }

	    ai++;
	    bi++;
	    div += w;
	}
	while (ai < al || bi < bl) {
	    double wa = ((double) ai) / al;
	    double wb = ((double) bi) / bl;
	    double w = Math.exp(-Math.min(wa, wb)) * wordWeight / 2;
	    if (bi < bl && street.charAt(bi) == ' ' || ai < al && ref.charAt(ai) == ' ') {
		wordWeight = wordWeight / 2;
	    }
	    ai++;
	    bi++;
	    div += w;
	}
	double v = ((double) s) / div;
	return v;
    }
}
