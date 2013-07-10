package ca.burnison.kijiji.contest;

import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.collect.Sets;

/**
 * Intended for use in geographical locations that primarily use a Latin1 character set for addresses. Specifically,
 * this parser assumes that all names are in the standard English alphabet. It normalizes all inbound address fields to
 * improve the matching abilities. This implementation uses simple String logic instead of regex, as it seems to
 * decrease execution times by over 50% and reduces CPU load by about 20%.
 * <p>
 * It was determined empirically that there is no need to retain any numeric characters when normalizing the address.
 * Because <i>most</i> streets don't have numbers in their name, this invariant holds true for most purposes. This is
 * an implementation decision and not a requirement. It's worth noting this strategy wouldn't work in cities that use
 * numbered streets (eg. 6th Avenue)
 */
@ThreadSafe
public final class Latin1AddressParser implements AddressParser
{
    /**
     * Simple cardinality registry.
     */
    private static final Set<String> CARDINALITIES = Sets.newHashSet(
        "N", "NORTH",
        "E", "EAST",
        "W", "WEST",
        "S", "SOUTH"
    );

    /**
     * After the directional suffix is removed, the street type will be last. Depending on the geographic locality, this
     * suffix list may not hold true.
     */
    private static final Set<String> TYPES = Sets.newHashSet(
        "AV",       "AVE",      "AVENUE",
        "BL",       "BVD",      "BLVD",     "BOUL",     "BOULEVARD",
        "CI",       "CIR",      "CLE",      "CRCL",     "CIRC",         "CIRCLE",
        "CR",       "CRS",      "CRES",     "CRESENT",  "CRESCENT",
        "CT",       "CRT",      "COURT",
        "DR",       "DRIVE",
        "GD",       "GDN",      "GDS",      "GDNS",     "GARDENS",
        "LA",       "LN",       "LANE",
        "LP",       "LOOP",
        "PK",       "PKW",      "PKWY",     "PRK",
        "PL",       "PLC",      "PLACE",
        "RD",       "ROAD",
        "SQ",       "SQUARE",
        "ST",       "STREET",
        "TR",       "TER",      "TERR",     "TERRACE"
    );

    @Override
    public String parse(final String address)
    {
        if(address == null || address.isEmpty()){
            return null;
        }

        final String normalized = this.normalize(address);
        if (normalized == null){
            return null;
        } else if(!normalized.contains(" ")){
            return normalized;
        }

        final String directionless = this.removeStreetDirections(normalized);
        if (!directionless.contains(" ")) {
            return directionless;
        }

        return this.removeStreetType(directionless);
    }

    /**
     * Normalize the input string assuming a standard English character set. This specific implementation performs about
     * 50% more efficiently than a compiled regex replaceAll, given that it only supports the latin1 character set.
     * @param address
     * @return A trimmed, upper-cased string containing only letters, spaces, and apostrophies.
     */
    private String normalize(final String address)
    {
        final String prepared = address.trim();
        if(prepared.isEmpty()){
            return null;
        }

        final StringBuilder sb = new StringBuilder(prepared.length());
        for(final char c : prepared.toCharArray()){
            if(Character.isLetter(c) || c == ' ' || c == '\'' ){
                sb.append(c);
            }
        }

        if(sb.length() == 0){
            return null;
        }

        // Could have been an errant unit letter. This could result in some false positives.
        final String normalized = (sb.charAt(1) == ' ') ? sb.substring(2) : sb.toString();
        return normalized.trim().toUpperCase();
    }

    /**
     * Remose the cardinality indicator on the address. This implementation performs about 20% more efficiently than a
     * compiled regex counterpart.
     */
    private String removeStreetDirections(final String address)
    {
        final int last = address.lastIndexOf(" ");
        if(last >= 0){
            final String cardinality = address.substring(last + 1);
            if(CARDINALITIES.contains(cardinality)){
                return address.substring(0, last).trim();
            }
        }
        return address;
    }

    /**
     * Remove the street type from this address. This implementation performs about 10% more efficiently than a compiled
     * regex does.
     */
    private String removeStreetType(final String address)
    {
        final int last = address.lastIndexOf(" ");
        if(last >= 0){
            final String type = address.substring(last + 1);
            if(TYPES.contains(type)){
                return address.substring(0, last).trim();
            }
        }
        return address;
    }
}
