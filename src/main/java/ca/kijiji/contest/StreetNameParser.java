package ca.kijiji.contest;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

/**
 * Don't want to use regexps here because they are slower then our bytes (near 6 times slower).
 *
 * Note: tests for this class located in class StreetNameParseTest
 * User: Eugene Loykov
 * Date: 13.07.13
 * Time: 20:08
 */
public final class StreetNameParser {
    private static final Charset charset = StandardCharsets.UTF_8;
    private static final byte LETTER_A = 'a';
    private static final byte LETTER_Z = 'z';
    private static final byte LETTER_CAPITAL_A = 'A';
    private static final byte LETTER_CAPITAL_Z = 'Z';

    // This part is little tricky. We converting suffix words to long and then storing these longs
    // to set so we can check them later with O(1) complexity.
    //
    // IMPORTANT! this algorithm not working for words with length >8.
    private static final String[] SUFFIX_DIRECTION_WORDS = {
            "ST", "STREET",
            "AV", "AVE",
            "COURT", "CRT", "CT",
            "RD",
            "E", "EAST",
            "W", "WEST",
            "S", "SOUTH",
            "N", "NORTH"};
    private static final Set<Long> SUFFIX_DIRECTIONS_LONGS = new HashSet<>();

    static {
        for (final String word : SUFFIX_DIRECTION_WORDS) {
            final byte[] bytes = stringToRawBytes(word);
            SUFFIX_DIRECTIONS_LONGS.add(rawBytesToLong(bytes, 0, bytes.length - 1));
        }
    }

    private StreetNameParser() {
        //nothing
    }

    public static String parse(byte[] data, int startIndex, int endIndex) {
        final int start = findStart(data, startIndex, endIndex);
        final int finish = findFinish(data, start, endIndex);

        return new String(data, start, finish - start + 1, charset);
    }

    private static int findStart(byte[] data, int startIndex, int endIndex) {
        // need to find first word that contains letters (that will be first word of the street name)
        for (int i = startIndex; i <= endIndex; i++) {
            final byte bt = data[i];

            if (bt == Constants.WORD_SEPARATOR) {
                startIndex = i + 1;
            } else {
                if (isLetter(bt)) {
                    return startIndex;
                }
            }
        }

        return startIndex;
    }

    private static int findFinish(byte[] data, int startIndex, int endIndex) {
        for (int i = endIndex; i >= startIndex; i--) {
            final byte bt = data[i];
            if (bt == Constants.WORD_SEPARATOR) {
                if (suffixDirectionWord(data, i + 1, endIndex)) {
                    endIndex = i - 1;
                }   else {
                    return endIndex;
                }
            }
        }

        return endIndex;
    }

    private static boolean suffixDirectionWord(byte[] data, int startIndex, int endIndex) {
        return SUFFIX_DIRECTIONS_LONGS.contains(rawBytesToLong(data, startIndex, endIndex));
    }

    private static boolean isLetter(byte bt) {
        return ((bt >= LETTER_A) && (bt <= LETTER_Z)) || ((bt >= LETTER_CAPITAL_A) && (bt <= LETTER_CAPITAL_Z));
    }

    private static long rawBytesToLong(byte[] data, int startIndex, int endIndex) {
        long result = 0;
        for (int i = startIndex; i <= endIndex; i++) {
            result = result << 8;
            result += data[i];
        }

        return result;
    }

    private static byte[] stringToRawBytes(String string) {
        final byte[] bytes = new byte[string.length()];

        for (int i = 0; i < string.length(); i++) {
            bytes[i] = (byte) string.charAt(i);
        }

        return bytes;
    }

}
