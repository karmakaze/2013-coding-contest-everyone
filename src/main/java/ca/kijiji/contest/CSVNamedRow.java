package ca.kijiji.contest;

import java.util.HashMap;
import java.util.Map;


/**
 * Helper class to extract the field value from a split CSV by field name using the headers.
 */
public class CSVNamedRow {
	private Map<String, Integer> _fieldNameToIndex = new HashMap<String, Integer>();
	
	/**
	 * Initializes the helper by taking in an array of the header values.
	 * @param headers The array of the header values.
	 */
	public CSVNamedRow(String[] headers) {
		for (int i = 0; i < headers.length; ++i) {
			_fieldNameToIndex.put(headers[i], i);
		}		
	}

	/**
	 * Takes in a row and the name of the field you want to get.
	 * @param row The split CSV array.
	 * @param fieldName The name of the field you want to retrieve.
	 * @return The field value by field name.
	 */
	public String getField(String[] row, String fieldName) {
		return row[_fieldNameToIndex.get(fieldName)];
	}
	
	/**
	 * Takes in a row and the name of the field you want to get and converts it to an int.
	 * @param row The split CSV array.
	 * @param fieldName The name of the field you want to retrieve.
	 * @return The field's int value by field name.
	 * @throws NumberFormatException The field contains a non-integer value.
	 */
	public int getIntegerField(String[] row, String fieldName) throws NumberFormatException {
		try {
			return Integer.parseInt(this.getField(row, fieldName));
		}
		catch (NumberFormatException nfe) {
			String actualValue = this.getField(row, fieldName);
			throw new NumberFormatException(String.format("Unable to parse field %s: %s", fieldName, actualValue));
		}
	}
	
	/**
	 * Returns the internal map of field names to the index value.
	 */
	public String toString() {
		return _fieldNameToIndex.toString();
	}
}
