package ca.kijiji.contest;

public class StringUtils {
    public static boolean isNullOrBlank(String str) {
        return str == null || str.isEmpty() || str.trim().isEmpty();
    }
}
