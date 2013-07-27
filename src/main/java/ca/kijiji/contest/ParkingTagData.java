package ca.kijiji.contest;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParkingTagData {
	
	public String tag_number;
	public String date_of_infraction;
	public String infraction_code;
	public String infraction_description;
	public String set_fine_amount;
	public String time_of_infraction;
	public String location1, location2, location3, location4;
	public String province;
	
	public ParkingTagData() {
		super();
	}
	
	public String streetNameFromLocation2() {
		return streetNameFromLocationStringUsingRegex(location2);
	}
	
	public String streetNameFromLocationStringUsingRegex(String str) {
		// Using Canada Post's Find a Postal Code page as a rough reference for address components
		
		final String streetNumberPatternString = "([\\d$-]*)";
		final String streetNamePatternString = "([a-zA-Z ']+)";
		final String streetTypePatternString = 
				"(AV|AVE|BLVD|CIR|CIRCLE|CRCL|CRT|CRES|CT|DR|DRIVE|GARDENS|GDNS|GRV|GRDNS|GT|HILL|HTS|LANE|LN|MEWS|PARKWAY|PKWY|PL|PROMENADE|QUAY|RD|ROAD|SQ|ST|TER|TRL|VISTA|WAY)";
		final String streetDirectionPatternString = "([NESW]?)";
		
		final Pattern locationPattern = Pattern.compile(
				streetNumberPatternString + " ?" +
				streetNamePatternString + " ?" +
				streetTypePatternString + " ?" +
				streetDirectionPatternString
				);
		
		Matcher matcher = locationPattern.matcher(str);
    	if (matcher.matches()) {
    		//System.out.println(str + "," + matcher.group(2));
    		return matcher.group(2);
    	} else {
    		System.out.println(str + ",");
    		return null;
    	}
	}
	
	public String toString() {
		return String.format("%s,%s", set_fine_amount, location2);
	}

}
