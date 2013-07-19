package ca.kijiji.contest;

public class StringUtils {
    public static boolean isNullOrBlank(String str) {
        return str == null || str.trim().isEmpty();
    }

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
