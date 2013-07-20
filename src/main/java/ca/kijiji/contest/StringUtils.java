package ca.kijiji.contest;

public class StringUtils {
    public static boolean isNullOrBlank(String str) {
        return str == null || str.trim().isEmpty();
    }

    // The split / join implementations from Apache Commons are much faster / better
    // for our purposes than Guava Joiners / Splitters. No need to massage arrays.
    public static String[] split(String str, char sep) {
        return org.apache.commons.lang.StringUtils.split(str, sep);
    }

    public static String[] splitPreserveNulls(String str, char sep) {
        return org.apache.commons.lang.StringUtils.splitPreserveAllTokens(str, sep);
    }

    public static String join(String[] toks, char separator, int end) {
        return org.apache.commons.lang.StringUtils.join(toks, separator, 0, end);
    }
}
