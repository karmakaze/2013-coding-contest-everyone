package ca.kijiji.contest;

import java.util.HashMap;
import java.util.Map;

/**
 * Params of the raw data (field indexes, names etc.)
 * <p/>
 * User: Eugene Loykov
 * Date: 13.07.13
 * Time: 18:56
 */
public final class RawDataParams {
    public static final String AMOUNT_FIELD_NAME = "set_fine_amount";
    public static final String STREET_FIELD_NAME = "location2";
    public static final String TAG_NUMBER_NAME = "tag_number_masked";
    public static final String DATE_OF_INFRACTION_NAME = "date_of_infraction";
    public static final int TAG_NUMBER_LENGTH = 8;
    public static final int DATE_OF_INFRACTION_LENGTH = 8;

    private final Map<String, Integer> paramNameToIndex = new HashMap<>();

    public void set(String name, int index) {
        paramNameToIndex.put(name, index);
    }

    public int getIndex(String name) {
        final Integer index = paramNameToIndex.get(name);
        return index == 0 ? -1 : index;
    }

}
