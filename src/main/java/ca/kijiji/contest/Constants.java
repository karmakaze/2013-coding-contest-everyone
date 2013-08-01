package ca.kijiji.contest;

/**
 * Some useful constants.
 * <p/>
 * User: Eugene Loykov
 * Date: 13.07.13
 * Time: 18:51
 */
public final class Constants {
    public static final byte EOL_CODE_BYTE = 10;
    public static final byte ZERO_BYTE = '0';
    public static final int EOL_CODE_INT = EOL_CODE_BYTE;
    public static final String DATA_SEPARATOR_STR = ",";
    public static final byte DATA_SEPARATOR_BYTE = (byte) ',';
    public static final byte WORD_SEPARATOR = ' ';

    public static int parseAmount(byte[] data, int startIndex, int endIndex) {
        int value = 0;
        for (int i = startIndex; i <= endIndex; i++) {
            value = value * 10 + (data[i] - Constants.ZERO_BYTE);
        }
        return value;
    }

}
