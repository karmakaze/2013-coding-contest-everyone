package ca.burnison.kijiji.contest;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Basic contract to allow for per-locale address parsing. This allows for a low complexity implementation that may be
 * optimized for a specific city. This abstraction may also allows for any level of verification, including network- or
 * process-based address cleansing/validation services.
 * <p>
 * Ideally, implementations will normalize the result such that the same street will be equal regardless of case or
 * padding. For example, "king", "King", and "   KING " should all be returned as "KING".
 * <p>
 * <b>Implementations must be thread safe.</b>
 */
@ThreadSafe
public interface AddressParser
{
    /**
     * Given the specified string representation of an address line, return a normalized address.
     * @param address
     * @return The new ticket or null if it could not be parsed.
     */
    public String parse(final String address);
}
