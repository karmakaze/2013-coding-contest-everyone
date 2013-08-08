package ca.kijiji.contest;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.*;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang.StringUtils;

/**
 * Resolves addresses to street names
 * Uses thread-safe caching internally.
 */
class StreetNameResolver {

    // Regex to separate the street number(s) from the street.
    // There need not be a street number, but it must be a combination of digits, certain punctuation and lowercase letters,
    // optionally followed by a single uppercase letter (ex. "123/345", "12451&2412", "2412C", "33-44", "235-a", "22, 77b")
    // Also handles junk street numbers (like "222-", "-33", "!33", "1o2", "l22"). OCR must have been used for some of the
    // data entry since o's and l's are mixed with 0s and 1s. We consider lower-case letters to be part of the street number
    // since that's the only place they show up, though one upper-case letter may appear on the *end* of a number (for
    // the unit.) This will erroneously match the "2E" of streets like "2E AV", but no such streets exist in Toronto.
    // This could be complete garbage like "aaaaa" but we don't really care so long as the street name's valid.
    private static final String STREET_NUM_REGEX = "(?<num>[\\p{N}\\p{Ll}\\-&/,\\. ]*\\p{Lu}?)";

    // Street names, designations and directions are all upper-case. Will also capture the UNIT if one is on the address,
    // use UNIT_REGEX if you need to get / remove it.
    private static final String STREET_REGEX = "(?<street>[\\p{N}\\p{L} #'\\-\\.]*)";

    // Ignore garbage at the beginning and end of the string and pull out the street numbers / names
    // Whoever released this data set as-is is a sadist.

    // Types of addresses this will fail on:
    // Compound addresses ("FOO ST & BAR ST", "QUUX ST / BAZ RD")
    // Containing data that should be in location3 or 4 ("BAY ST N/O", "BACK/OF   425 ADELAIDE ST W")
    // Addresses with non-standard characters ("90 ,PRME;;E CT") as they are usually accidental
    // Addresses with non-standard data ("1240 ISLINGTON AV KISS & RIDE M8X 141")

    // We do allow:
    // Garbage before, between and after the street ("#221 $$$$FOO ST######") which gets discarded
    private static final Pattern ADDRESS_REGEX =
            Pattern.compile("^[^\\p{N}\\p{L}]*(" + STREET_NUM_REGEX + "[^\\p{N}\\p{L}]*\\s+)?" + STREET_REGEX + "[^\\p{N}\\p{L}]*$");

    // Get or a remove a unit number from an address
    // https://www.canadapost.ca/tools/pg/manual/PGaddress-e.asp#1380473
    private static final Pattern UNIT_REGEX =
            Pattern.compile("^(?<street>.*)\\s+(?<unittype>UNIT|UNITÉ|APT|APP|SUITE|BUREAU)\\s+#?\\s*(?<unit>\\d+)$");

    // Set of directions a street may end with
    private static final ImmutableSet<String> DIRECTION_SET = ImmutableSet.of(
            //"NS" means either North *or* South? Only shows up in a couple of places
            "N", "NORTH", "S", "SOUTH", "W", "WEST", "E", "EAST", "NE", "NW", "SW", "SE", "NS"
    );

    // Set of designators to remove from the end of street names (ST, ROAD, etc.)
    // The designation may be necessary for disambiguation (YONGE BLVD vs YONGE ST,) and sometimes the
    // designation is a significant part of the name (THE ESPLANADE or THE BOULEVARD) so it'd be *better*
    // to normalize the designation, but the test case requires no designations.
    private static final ImmutableSet<String> DESIGNATION_SET = ImmutableSet.of(
            // Taken from
            // `cut -d, -f8 Parking_Tags_Data_2012.csv | sed 's/\s+$//g' | awk -F' ' '{print $NF}' | sort | uniq -c | sort -n`
            // and https://www.canadapost.ca/tools/pg/manual/PGaddress-e.asp#1423617
            "ABBEY", "ACRES", "ALLEY", "ALLÉE", "AUT", "AUTOROUTE", "AV", "AVE", "AVENUE", "BAY", "BEACH", "BEND", "BL", "BLV",
            "BLVD", "BOULEVARD", "BY-PASS", "BYPASS", "BYWAY", "C", "CAMPUS", "CAPE", "CAR", "CARREF", "CARREFOUR", "CARRÉ",
            "CDS", "CENTRE", "CERCLE", "CH", "CHASE", "CHEMIN", "CIR", "CIRCLE", "CIRCT", "CIRCUIT", "CLOSE", "COMMON", "CONC",
            "CONCESSION", "CORNERS", "COUR", "COURS", "COURT", "COVE", "CR", "CRCL", "CRCT", "CRES", "CRESCENT", "CRNRS",
            "CROIS", "CROISSANT", "CROSS", "CROSSING", "CRS", "CRST", "CRT", "CT", "CUL-DE-SAC", "CÔTE", "D", "DALE", "DELL",
            "DIVERS", "DIVERSION", "DOWNS", "DR", "DRIVE", "ÉCH", "ÉCHANGEUR", "END", "ESPL", "ESPLANADE", "ESTATE", "ESTATES",
            "EXPRESSWAY", "EXPY", "EXTEN", "EXTENSION", "FARM", "FIELD", "FOREST", "FREEWAY", "FRONT", "FWY", "GARDEN",
            "GARDENS", "GATE", "GDN", "GDNS", "GLADE", "GLEN", "GR", "GRDNS", "GREEN", "GRNDS", "GROUNDS", "GROVE", "GRV",
            "GT", "HARBOUR", "HARBR", "HEATH", "HEIGHTS", "HGHLDS", "HGHTS", "HIGHLANDS", "HIGHWAY", "HILL", "HOLLOW",
            "HTS", "HWY", "IMP", "IMPASSE", "INLET", "ISLAND", "ÎLE", "KEY", "KNOLL", "LANDING", "LANDNG", "LANE",
            "LIMITS", "LINE", "LINK", "LKOUT", "LMTS", "LN", "LOOKOUT", "LOOP", "MALL", "MANOR", "MAZE", "MEADOW", "MEWS",
            "MONTÉE", "MOOR", "MOUNT", "MOUNTAIN", "MTN", "ORCH", "ORCHARD", "PARADE", "PARC", "PARK", "PARKWAY", "PASS",
            "PASSAGE", "PATH", "PATHWAY", "PINES", "PK", "PKWY", "PKY", "PL", "PLACE", "PLAT", "PLATEAU", "PLAZA", "PLCE",
            "POINT", "POINTE", "PORT", "PRIVATE", "PRK", "PROM", "PROMENADE", "PT", "PTWAY", "PVT", "QUAI", "QUAY", "RAMP",
            "RANG", "RANGE", "RD", "RDPT", "RG", "RIDGE", "RISE", "RLE", "ROAD", "ROND-POINT", "ROUTE", "ROW", "RTE", "RUE",
            "RUELLE", "RUN", "SENT", "SENTIER", "SQ", "SQUARE", "ST", "STR", "STREET", "SUBDIV", "SUBDIVISION", "T", "TER",
            "TERR", "TERRACE", "TERRASSE", "THICK", "THICKET", "TLINE", "TOWERS", "TOWNLINE", "TR", "TRAIL", "TRL", "TRNABT",
            "TSSE", "TURNABOUT", "V", "VALE", "VIA", "VIEW", "VILLAGE", "VILLAS", "VILLGE", "VISTA", "VOIE", "WALK", "WAY",
            "WHARF", "WOOD", "WY", "WYND"
    );

    // Map of cache-friendly addresses to their respective street names
    private final Map<String, String> _mStreetCache = new ConcurrentHashMap<>();

    public StreetNameResolver() {

    }

    /**
     * Get a street name from a trim()ed address
     * @param address the trim()ed address to parse a street name from (ex: "123 FAKE ST W")
     * @return a street name (ex: "FAKE") or null if a street name couldn't be parsed out
     */
    public String addressToStreetName(CharRange address) {

        // Try to remove the street number from the front so we're more likely to get a cache hit
        String streetCacheKey = _getAddressCacheKey(address);

        // Check if we have a cached name
        String streetName = _mStreetCache.get(streetCacheKey);

        // No cached street name, calculate it
        if(streetName == null) {

            // Split the address into street number and street components
            Matcher addressMatcher = ADDRESS_REGEX.matcher(address.toString());

            // Yep, this looks like an address
            if(addressMatcher.matches()) {

                // Pull the street out of the address
                String street = addressMatcher.group("street");

                // The street address *may* have a unit number on the end, remove it.
                Matcher unitMatcher = UNIT_REGEX.matcher(street);
                if(unitMatcher.matches()) {
                    // Take everything before the unit number
                    street = unitMatcher.group("street");
                }

                // Get just the street *name* from the street
                streetName = _getStreetNameFromStreet(street);

                // Reject street names that are *entirely* comprised of numbers and / or whitespace
                if(StringUtils.isNumericSpace(streetName)) {
                    return null;
                }

                // Add the street name to the cache. We don't really care if this gets clobbered,
                // we put in the same val for a key no matter what.
                _mStreetCache.put(streetCacheKey, streetName);
            }
        }

        return streetName;
    }

    /**
     * Get a cacheable version of this address for street name lookups by trying to
     * lop off the street number using simple operations.
     * Results in at least a 17% speed increase over always running the full street name parser.
     *
     * This optimizes for the common case of NUMBER? STREET DESIGNATION? DIRECTION? with no garbage.
     *
     * Number of addresses beginning with (what looks like) street numbers
     * $ cut -d, -f8 src/test/resources/Parking_Tags_Data_2012.csv | grep -P '^\d[\da-z]*\s' | wc -l
     * 2603723
     * Number of addresses beginning with (what looks like) street names
     * $ cut -d, -f8 src/test/resources/Parking_Tags_Data_2012.csv | grep -P '^[A-Z]' | wc -l
     * 138405
     *
     * 2742128 / 2746155
     *
     * This allows us to do expensive operations for the sake of accuracy in the full parser without
     * taking too much of a performance hit (only around 20,000 addresses miss the cache on average)
     * We also prefer a cache miss to a false cache hit, for example:
     * "1B YONGE ST" won't be modified and will likely result in a cache miss to properly handle "12TH ST"
     *
     * @param address a trim()ed street address
     * @return a suitable cache key for this address.
     */
    private static String _getAddressCacheKey(CharRange address) {

        // charAt() doesn't work right with surrogate pairs,
        // but there aren't any hieroglyphics in the input, so...
        if(Character.isDigit(address.charAt(0))) {

            // Check where the first space is
            int space_idx = address.indexOf(' ');

            // If there's a space in the address
            if(space_idx != -1) {

                // Get the last character in the first token
                char lastChar = address.charAt(space_idx - 1);

                // A token at the start of the list starting with a digit and ending with a
                // number or lowercase letter is *always* a street number
                if(Character.isDigit(lastChar) || Character.isLowerCase(lastChar)) {
                    // Return all of the tokens after (what I hope is) the street number
                    return address.strSlice(space_idx + 1);
                }
            }
        }

        // This is probably a street name, return as-is
        return address.toString();
    }

    /**
     * Get *just* the street name from a street
     * @param street the street to process (ex: "FOO ST W")
     * @return the processed street name (ex: "FOO")
     * */
    private String _getStreetNameFromStreet(String street) {

        // Split the street up into tokens
        String[] streetTokens = StringUtils.split(street, ' ');

        // Go backwards through the tokens and skip those that aren't likely part of the actual name.
        int lastNameTokenIdx = 0;

        for(int i = streetTokens.length - 1; i >= 0; --i) {
            String token = streetTokens[i];

            // Index of the last token in the street name proper
            lastNameTokenIdx = i;

            // Cut off the terminating period if there is one so we can recognize "ST." and "W."
            if(token.endsWith(".")) {
                token = StringUtils.chop(token);

                // The string only contained a period, continue to the next token.
                if(token.isEmpty()) {
                    continue;
                }
            }

            // There may be multiple direction tokens (N E, S E, etc.) but they never show up before a
            // street designation. Stop looking at tokens as soon as we hit the first token that looks
            // like a street designation, otherwise we'll mangle names like "HILL STREET" or "GREEN HILL CRESCENT".
            // According to Canada Post, streets only have one designation and there are no multi-word designations.
            // https://www.canadapost.ca/tools/pg/manual/PGaddress-e.asp#1423617

            // Don't mangle streets like "GROVE" with no designator.
            if(DESIGNATION_SET.contains(token) && i != 0) {
                // Designation token, stop reading.
                break;
            } else if(DIRECTION_SET.contains(token) && i != 0) {
                // Direction token, keep going til we hit a designation or name token.
                continue;
            } else {
                // This token is neither a direction nor a designation, this is part of the street name! Bail out.
                // join's range is non-inclusive, increment it so this token is included in the result
                ++lastNameTokenIdx;
                break;
            }
        }

        // join together the tokens that make up the street's name and return
        return StringUtils.join(streetTokens, ' ', 0, lastNameTokenIdx);
    }
}
