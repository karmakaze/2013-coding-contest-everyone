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
		return str;
	}
	
	public String toString() {
		return String.format("%s,%s", set_fine_amount, location2);
	}

}
