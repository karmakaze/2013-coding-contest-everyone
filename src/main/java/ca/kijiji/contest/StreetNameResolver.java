package ca.kijiji.contest;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang.StringUtils;

/**
 * Resolves addresses to street names
 * Uses thread-safe caching internally.
 */
public class StreetNameResolver {

    // Regex to separate the street number from the street.
    // There need not be a street number, but it must be a combination of digits, punctuation and lowercase letters.
    // optionally followed by a single uppercase letter (ex. "123/345", "12451&2412", "2412J", "33-44", "235-a", "22, 77b")
    // Also handles junk street numbers (like "222-", "-33", "!33", "1o2", "l22"). OCR must have been used for some of the
    // data entry since o's and l's are mixed with 0s and 1s. We consider lower-case letters to be part of the street number
    // since that's the only place they show up, though one upper-case letter may appear on the *end* of a number (for
    // the unit.) This will erroneously match the "2E" of streets like 2E AVENUE, but no such streets exist in Toronto.
    private static final String STREET_NUM_REGEX = "(?<num>[\\p{N}\\p{Ll}\\-&/,\\. ]*\\p{Lu}?)";

    // Street names, designations and directions are all upper-case. Fails on streets with periods in the name proper
    // in favor of discarding periods at the end of street designation abbreviations.
    private static final String STREET_REGEX = "(?<street>[\\p{N}\\p{L} '-]*)\\.?";

    // Ignore garbage at the beginning and end of the string and pull out the street numbers / names
    // Whoever released this data set as-is is a sadist.
    private static final Pattern ADDRESS_REGEX =
            Pattern.compile("^[^\\p{N}\\p{L}]*(" + STREET_NUM_REGEX + "[^\\p{N}\\p{L}]*\\s+)?" + STREET_REGEX + ".*");

    // Set of directions a street may end with
    private static final ImmutableSet<String> DIRECTION_SET = ImmutableSet.of(
            //"NS" means either North *or* South? Only shows up in a couple of places
            "N", "NORTH", "S", "SOUTH", "W", "WEST", "E", "EAST", "NE", "NW", "SW", "SE", "NS"
    );

    // Set of designators to remove from the end of street names (ST, ROAD, etc.)
    // The designation may be necessary for disambiguation (YONGE BLVD vs YONGE ST,) so it'd be *better*
    // to normalize the designation, but the test case requires no designations.
    private static final ImmutableSet<String> DESIGNATION_SET = ImmutableSet.of(
            // mostly from
            // cut -d, -f8 Parking_Tags_Data_2012.csv | sed 's/\s+$//g' | awk -F' ' '{print $NF}' | sort | uniq -c | sort -n
            "AV", "AVE", "AVENUE", "BL", "BLV", "BLVD", "BOULEVARD", "CIR", "CIRCLE", "CIRCUIT", "CR", "CRCL", "CRCT",
            "CRES", "CRS", "CRST", "CRESCENT", "CT", "CRT", "COURT", "D", "DR", "DRIVE", "GATE", "GARDEN", "GDN", "GDNS",
            "GARDENS", "GR", "GRDNS", "GROVE", "GRV", "GT", "HGHTS", "HEIGHTS", "HTS", "HILL", "LN", "LANE", "MANOR", "MEWS",
            "PARK", "PARKWAY", "PK", "PKWY", "PRK", "PL", "PLCE", "PLACE", "PROMENADE", "QUAY", "RD", "ROAD", "ST", "STR",
            "SQ", "SQUARE", "STREET", "T", "TER", "TERR", "TERRACE", "TR", "TRL", "TRAIL", "VISTA", "V", "WAY", "WY", "WOOD"
    );

    // Number of lookups that could be done via cache lookup.
    // If we had more tickets or did multiple passes this would required a LongAdder.
    AtomicInteger _mCacheHits = new AtomicInteger(0);

    // Map of cache-friendly addresses to their respective street names
    private final Map<String, String> _mStreetCache = new ConcurrentHashMap<>();

    public StreetNameResolver() {

    }

    /**
     * Get a street name from an address
     * @param address the address to parse a street name from (ex: "123 FAKE ST W")
     * @return a street name (ex: "FAKE") or null if a street name couldn't be parsed out
     */
    public String addressToStreetName(String address) {

        // Try to remove the street number from the front so we're more likely to get a cache hit
        String streetCacheKey = _getCacheableAddress(address);

        // We have a valid cache key, check if we have a cached name
        String streetName = _mStreetCache.get(streetCacheKey);

        // No cached street name, calculate it
        if(streetName == null) {

            // Split the address into street number and street components
            Matcher addressMatcher = ADDRESS_REGEX.matcher(address);

            // Yep, this looks like an address
            if(addressMatcher.matches()) {

                // Get just the street *name* from the street
                streetName = _isolateStreetName(addressMatcher.group("street"));

                // No tokens left in the street name, it's likely invalid.
                if(streetName.isEmpty()) {
                    return null;
                }

                // Reject street names that are *entirely* comprised of numbers
                if(StringUtils.isNumericSpace(streetName)) {
                    return null;
                }

                // Add the street name to the cache. We don't really care if this gets clobbered,
                // we put in the same val for a key no matter what.
                _mStreetCache.put(streetCacheKey, streetName);
            }
        } else {
            _mCacheHits.getAndIncrement();
        }

        return streetName;
    }

    public int getCacheHits() {
        return _mCacheHits.intValue();
    }

    /**
     * Get a cacheable version of this address for street name lookups by lopping off the
     * Street number using simple operations (if we can.)
     * Results in a 17% speed increase over always running the full street name parser.
     *
     * This optimizes for the common case of NUMBER? STREET DESIGNATION? DIRECTION? with no garbage.
     * We also prefer a cache miss to a false cache hit, for example:
     * "1B YONGE ST" won't be modified and will likely result in a cache miss to properly handle "12TH ST"
     */
    private static String _getCacheableAddress(String address) {

        // charAt() probably doesn't work right with surrogate pairs.
        // but neither do our character checks
        if(Character.isDigit(address.charAt(0))) {

            // Check where the first space is
            int space_idx = address.indexOf(' ');

            // There's a space in the address
            if(space_idx != -1) {

                // An uppercase letter at the end of a token almost always means a street name.
                if(Character.isUpperCase(address.charAt(space_idx - 1))) {
                    // return as-is, then.
                    return address;
                }

                // Lop off (what I hope is) the street number and return the rest
                return address.substring(space_idx);
            }
        }

        //Doesn't start with a digit or has no spaces, probably starts with a street name
        return address;
    }

    /**
     * Get *just* the street name from a street
     * */
    private String _isolateStreetName(String street) {

        // Split the street up into tokens
        String[] streetTokens = StringUtils.split(street, ' ');

        // Go backwards through the tokens and skip all the ones that aren't likely part of the actual name.
        int lastNameTokenIdx = 0;

        for(int i = streetTokens.length - 1; i > -1; --i) {
            String token = streetTokens[i];

            // Index of the last token in the street name proper
            lastNameTokenIdx = i;

            // There may be multiple direction tokens (N E, S E, etc.) but they never show up before a
            // street designation. Stop looking at tokens as soon as we hit the first token that looks
            // like a street designation, otherwise we'll mangle names like "HILL STREET".
            // Streets like "GROVE" with no designator will get mangled, but junk in junk out.
            if(DESIGNATION_SET.contains(token)) {
                break;
            }
            // This token is neither a direction nor a designation, this is part of the street name!
            // Bail out.
            else if(!DIRECTION_SET.contains(token)) {
                // join's range is non-inclusive, increment it so this token is included in the street name
                ++lastNameTokenIdx;
                break;
            }
        }

        // join together the tokens that make up the street's name and return
        return StringUtils.join(streetTokens, ' ', 0, lastNameTokenIdx);
    }
}
