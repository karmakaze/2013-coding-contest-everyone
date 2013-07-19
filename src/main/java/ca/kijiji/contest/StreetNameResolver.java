package ca.kijiji.contest;

import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;

/**
 * Resolve an address to a street name
 * Uses thread-safe caching internally.
 */
public class StreetNameResolver {
    // Join the tokens back together
    private static final Joiner STREET_JOINER = Joiner.on(' ');

    // Regex to separate the street number from the street name
    // there need not be a street number, but it must be a combination of digits and punctuation with
    // an optional letter at the end for apartments. (ex. 123/345, 12451&2412, 2412a, 33-44, 235-a)
    // Also handles junk street numbers (like 222-, -33, !33, 1o2, l22) because they aren't important

    // Whoever released this dataset without cleaning it up is a sadist.
    private static final Pattern ADDR_REGEX =
            Pattern.compile("^[^\\p{N}\\p{L}]*((?<num>[\\p{N}ol\\-&/, ]*(\\p{N}(-?\\p{L}))?)\\s+)?(?<street>[\\p{N}\\p{L} \\.'-]+).*");

    // Set of directions a street may end with
    private static final ImmutableSet<String> DIRECTION_SET = ImmutableSet.of(
            //"NS" means either North *or* South? Only shows up in a couple of places
            "N", "NORTH", "S", "SOUTH", "W", "WEST", "E", "EAST", "NE", "NW", "SW", "SE", "NS"
    );

    // Set of designators to remove from the end of street names (ST, ROAD, etc.)
    // The designation may be necessary for disambiguation, so it'd be *better* to normalize the designation,
    // but this test requires no designations, so use a set of designations to ignore
    private static final ImmutableSet<String> DESIGNATION_SET = ImmutableSet.of(
            // mostly from the top of
            // `cut -d, -f8 Parking_Tags_Data_2012.csv | sed 's/\s+$//g' | awk -F' ' '{print $NF}' | sort | uniq -c | sort -n`
            "AV", "AVE", "AVENUE", "BL", "BLV", "BLVD", "BOULEVARD", "CIR", "CIRCLE", "CR", "CRCL", "CRCT", "CRES", "CRS",
            "CRST", "CRESCENT", "CT", "CRT", "COURT", "D", "DR", "DRIVE", "GARDEN", "GDN", "GDNS", "GARDENS", "GR", "GRDNS",
            "GROVE", "GRV", "GT", "HGHTS", "HEIGHTS", "HTS", "HILL", "LN", "LANE", "MANOR", "MEWS", "PARKWAY", "PK", "PKWY",
            "PRK", "PL", "PLCE", "PLACE", "PROMENADE", "QUAY", "RD", "ROAD", "ST", "STR", "SQ", "SQUARE", "STREET", "T", "TER",
            "TERR", "TERRACE", "TR", "TRL", "TRAIL", "VISTA", "V", "WAY", "WY", "WOOD"

    );

    private ConcurrentHashMap<String, String> _mStreetCache = new ConcurrentHashMap<>();

    public StreetNameResolver() {}

    /**
     * Get a street name (ex: FAKE) from an address (ex: 123 FAKE ST W)
     */
    public String addrToStreetName(String addr) {

        String streetName = null;

        // Try to remove the street number from the front so we're more likely to get a cache hit
        String streetCacheKey = _getCacheableAddress(addr);

        if(streetCacheKey != null)
            streetName = _mStreetCache.get(streetCacheKey);

        // No normalized version in the cache, calculate it
        if(streetName == null) {

            // Split the address into street number and street name components
            Matcher addrMatches = ADDR_REGEX.matcher(addr);

            // Hmmm, this doesn't really look like an address...
            if(!addrMatches.matches()) {
                return null;
            }

            // Get just the street *name* from the street
            streetName = _isolateStreetName(addrMatches.group("street"));

            // Add the street name to the cache if it's cacheable
            if(streetCacheKey != null) {
                _mStreetCache.put(streetCacheKey, streetName);
            }
        }

        return streetName;
    }

    /**
     * Get a more cacheable version of this address for street name lookup by cutting off the
     * Street number if there is one.
     * Results in a 14.5% speed increase over always running the regex and using group("street").
     */
    private String _getCacheableAddress(String addr) {
        // Regex matches are expensive! Try to get a cache hit without one.
        if(Character.isDigit(addr.charAt(0))) {

            // Get everything after the first space if there is one
            int space_idx = addr.indexOf(' ');

            if(space_idx == -1) {
                return null;
            }

            // A letter on the end of the number may be an apartment number, but
            // A letter as the  means it's likely part of the name (an address like "12TH ST" or "123RD ST")
            if (space_idx > 2) {
                if(Character.isLetter(addr.charAt(space_idx - 1))) {
                    return addr;
                }
            }

            // Lop off (what I hope is) the street number and return the rest
            return addr.substring(space_idx);
        }

        return null;
    }

    /**
     * Get *just* the street name from a street
     * */
    private String _isolateStreetName(String street) {
        // Split the street up into tokens (may contain
        String[] streetToks = StringUtils.split(street, ' ');

        // Go backwards through the tokens and skip all the ones that aren't likely part of the actual name.
        int lastNameElem = 0;

        for(int i = streetToks.length - 1; i >= 0; --i) {
            String tok = streetToks[i];

            lastNameElem = i;

            // There may be multiple direction tokens (N E, S E, etc.) but they never show up before a
            // street designation. Stop looking at tokens as soon as we hit the first token that looks
            // like a street designation otherwise we'll mangle names like "HILL STREET"
            if(DESIGNATION_SET.contains(tok)) {
                break;
            }
            // This is neither a direction nor a designation, this is part of the street name!
            // Bail out.
            if(!DIRECTION_SET.contains(tok)) {
                // join's range is non-inclusive, increment it so this token is included in the street name
                ++lastNameElem;
                break;
            }
        }

        // join together the tokens that make up the street's name and return
        return StringUtils.join(streetToks, ' ', lastNameElem);
    }
}
