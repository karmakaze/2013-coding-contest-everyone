package ca.kijiji.contest;

import java.util.Arrays;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParkingTagData {
	
	//			  field name                field length
	public String tag_number;				// 8
	public String date_of_infraction;		// 8
	public String infraction_code;			// *
	public String infraction_description;	// *
	public String set_fine_amount;			// *
	public String time_of_infraction;		// 4
	public String location1;				// *
	public String location2;				// *
	public String location3;				// *
	public String location4;				// *
	public String province;					// 2
	
	static {
		String streetNumberPatternString = "([i\\d$\\(/-]*)";
		String streetNamePatternString = "([a-zA-Z '-]+)";
		String streetTypePatternString = 
				"(AV|AVE|AVENUE|BL|BLVD|BVLD|CIR|CIRC|CIRCL|CIRCLE|CIRT|CIRCUIT|COURT|CR|CRCL|CRT|CRES|CT|DR|DRIVE|GARDEN|GARDENS|GDNS|GR|GRV|GRDNS|GROVE|GT|HILL|HTS|KEEP|LANE|LINE|LODGE|LN|LWN|MALL|MEWS|PARKWAY|PATH|PARK|PK|PKWY|PL|PLACE|POINT|PROMENADE|PT|PTWY|QUAY|RAOD|RD|ROAD|ROWN|SQ|SQUARE|ST|STREET|TER|TERR|TERRACE|TR|TRAIL|TRL|VIEW|VISTA|WALK|WAY|WAYS|WOOD)\\.?";
		String streetDirectionPatternString = "([NESW]?)";
		
		digitPattern = Pattern.compile("\\p{Nd}");
		locationPattern = Pattern.compile(
			streetNumberPatternString + " ?" +
			streetNamePatternString + " ?" +
			streetTypePatternString + " ?" +
			streetDirectionPatternString
		);
	}
	
	static HashSet<String> streetTypes = new HashSet<String>(Arrays.asList(
		new String[] {"AVE", "AVENUE", "BLVD", "BVLD", "CIR", "CIRC", "CIRCL", "CIRCLE", "CIRT", "CIRCUIT", "COURT", "CRCL", "CRT", "CRES", "DRIVE", "GARDEN", "GARDENS", "GDNS", "GRV", "GRDNS", "GROVE", "HILL", "HTS", "KEEP", "LANE", "LINE", "LODGE", "LWN", "MALL", "MEWS", "PARKWAY", "PATH", "PARK", "PKWY", "PLACE", "POINT", "PROMENADE", "PTWY", "QUAY", "RAOD", "RD", "ROAD", "ROWN", "SQUARE", "STREET", "TER", "TERR", "TERRACE", "TRAIL", "TRL", "VIEW", "VISTA", "WALK", "WAY", "WAYS", "WOOD"}
	));
	
	static Pattern locationPattern;
	static Pattern digitPattern;
			
	private Matcher locationMatcher = locationPattern.matcher("");
	private Matcher digitMatcher = digitPattern.matcher("");
	
	public ParkingTagData() {
		super();
	}
	
	/**
	 * Parses a line from a parking tickets data file and updates this object with
	 * the extracted data. <code>significantFieldsOnly</code> indicates whether this
	 * method should extract all the fields in the line, or only the fields required
	 * for the coding challenge (set_fine_amount and location2). Returns <code>true</code>
	 * if data is extracted, <code>false</code> otherwise. 
	 * 
	 * @param line	a line of data from a parking tickets data file 
	 * @param significantFieldsOnly	whether to extract all fields, or only fields
	 * 								significant to the contest
	 * @return whether data was successfully extracted
	 */
	public boolean updateFromDataLine(String line, boolean significantFieldsOnly) {
		if (line == null) {
			return false;
		}
		
		// If significantFieldsOnly is true, then the only fields we care about
		// for the contest are set_fine_amount and location2. start and end indicate
		// the start and end indices of interesting parts of the line. They're initialized
		// to 18 to start because the first two fields (tag_number_masked and
		// date_of_infraction) have fixed lengths and we don't need them, so we'll skip
		// directly passed them. 
		if (significantFieldsOnly) {
			int start = 18;
			int end = 18;
			
			// Find the range of the set_fine_amount field. We're starting from the
			// third field (infraction_code), so we'll scan past two commas to reach the
			// fifth field (set_fine_amount).
			start = line.indexOf(',', start) + 1;
			start = line.indexOf(',', start) + 1;
			end = line.indexOf(',', start);
			
			set_fine_amount = line.substring(start, end);
			
			// Find the range of the location2 field. It's another three fields in.
			start = line.indexOf(',', end) + 1;
			start = line.indexOf(',', start) + 1;
			start = line.indexOf(',', start) + 1;
			end = line.indexOf(',', start);
			
			location2 = line.substring(start, end);
		}
		
		// Otherwise, if significantFieldsOnly is false, then split the line by commas
		// and fill in all the tag data.
		else {
			String[] fields = line.split(",");
			
			tag_number = fields[0];
			date_of_infraction = fields[1];
			infraction_code = fields[2];
			infraction_description = fields[3];
			set_fine_amount = fields[4];
			time_of_infraction = fields[5];
			location1 = fields[6];
			location2 = fields[7];
			location3 = fields[8];
			location4 = fields[9];
			province = fields[10];
		}
		
		return true;
	}
	
	/*
	 * Returns set_fine_amount as an Integer.
	 */
	public Integer fineAmount() {
		return new Integer(set_fine_amount);
	}
	
	/*
	 * Extracts the street name from location2 using a regular expression.
	 */
	public String streetNameFromLocation2UsingRegex() {
		// Using Canada Post's Find a Postal Code page as a rough reference for address components
		
		//Matcher matcher = locationPattern.matcher(location2);
		locationMatcher.reset(location2);
    	if (locationMatcher.matches()) {
    		//System.out.println(str + "," + matcher.group(2));
    		return locationMatcher.group(2).trim();
    	} else {
    		//System.out.println(str + ",");
    		return null;
    	}
	}
	
	/*
	 * Extracts the street name from location2 by scanning through the string
	 * and looking for likely start and end points for the street name.
	 */
	public String streetNameFromLocation2ByScanning() {
		return null;
	}
	
	/* 
	 * Extracts the street name from location2 by splitting the string by
	 * spaces and picking out a subarray that is likely the street name 
	 */
	public String streetNameFromLocation2BySplitting() {
		String[] components = location2.split(" ");
		
		// From the start of the components, eliminate anything containing numbers
		int start = 0;
		while (start < components.length && digitMatcher.reset(components[start]).find()) {
			start++;
		}
		
		// From the end of the components, eliminate anything less 2 characters or less,
		// or is in the set of street types
		int end = components.length - 1;
		while (end > start && (components[end].length() <= 2 || streetTypes.contains(components[end]))) {
			end--;
		}
		
		// The components from start to end should be the street name
		if (start == end) {
			return components[start];
		} else {
			StringBuilder sb = new StringBuilder();
			
			for (int i = start; i < end + 1; i++) {
				sb.append(components[i]);
				
				if (i < end) {
					sb.append(" ");
				}
			}
			
			return sb.toString();
		}
	}
	
	public String toString() {
		return String.format("%s,%s", set_fine_amount, location2);
	}

}
