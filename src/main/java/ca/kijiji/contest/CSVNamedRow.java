package ca.kijiji.contest;

import java.util.HashMap;
import java.util.Map;

public class CSVNamedRow {
	private Map<String, Integer> _fieldNameToIndex = new HashMap<String, Integer>();
	
	public CSVNamedRow(String[] headers) {
		for (int i = 0; i < headers.length; ++i) {
			_fieldNameToIndex.put(headers[i], i);
		}		
	}

	public String getField(String[] row, String fieldName) {
		return row[_fieldNameToIndex.get(fieldName)];
	}
	
	public int getIntegerField(String[] row, String fieldName) throws NumberFormatException {
		return Integer.parseInt(this.getField(row, fieldName));
	}
	
	public String toString() {
		return _fieldNameToIndex.toString();
	}
}
