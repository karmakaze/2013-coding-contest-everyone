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
		
		final String streetNumberPatternString = "([i\\d$\\(/-]*)";
		final String streetNamePatternString = "([a-zA-Z '-]+)";
		final String streetTypePatternString = 
				"(AV|AVE|AVENUE|BL|BLVD|BVLD|CIR|CIRC|CIRCL|CIRCLE|CIRT|CIRCUIT|COURT|CR|CRCL|CRT|CRES|CT|DR|DRIVE|GARDEN|GARDENS|GDNS|GR|GRV|GRDNS|GROVE|GT|HILL|HTS|KEEP|LANE|LINE|LODGE|LN|LWN|MALL|MEWS|PARKWAY|PATH|PARK|PK|PKWY|PL|PLACE|POINT|PROMENADE|PT|PTWY|QUAY|RAOD|RD|ROAD|ROWN|SQ|SQUARE|ST|STREET|TER|TERR|TERRACE|TR|TRAIL|TRL|VIEW|VISTA|WALK|WAY|WAYS|WOOD)\\.?";
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
    		return matcher.group(2).trim();
    	} else {
    		//System.out.println(str + ",");
    		return null;
    	}
	}
	
	public String toString() {
		return String.format("%s,%s", set_fine_amount, location2);
	}

}
