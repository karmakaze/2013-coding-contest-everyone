package ca.kijiji.contest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class ParkingTagsReader {
	
	// Parking Tags Data CSVs have the following fields (length, name, significant):
	//
	// 8 tag_number_masked
	// 8 date_of_infraction
	// * infraction_code
	// * infraction_description
	// * set_fine_amount +
	// 4 time_of_infraction
	// * location1
	// * location2 +
	// * location3
	// * location4
	// 2 province
	
	private InputStream parkingTicketsStream = null;
	private BufferedReader parkingTicketsReader = null;
	private boolean shouldCaptureAllData = false;
	private boolean started = false;
	
	public ParkingTagsReader(InputStream parkingTicketsStream, boolean shouldCaptureAllData) {
		super();
		
		this.parkingTicketsStream = parkingTicketsStream;
		this.shouldCaptureAllData = shouldCaptureAllData;
	}
	
	public void start() throws IOException {
		if (started == false) {
			parkingTicketsReader = new BufferedReader(new InputStreamReader(parkingTicketsStream, "ascii"));
			
			// Throw away the first line (it has the column headers)
			parkingTicketsReader.readLine();
			
			started = true;
		}
	}

	/**
	 * Reads a line from parkingTicketsStream and parses out the data. This method expects
	 * a TagData object to be passed in that will hold the parsed tag data. Instead of
	 * allocating TagData objects for each line, this lets the caller pass in a TagData
	 * that can be used over and over again.
	 *  
	 * @param tag An object to hold the parsed tag data.
	 * @return Returns whether or not data was parsed and placed in tag.
	 * @throws IOException 
	 */
	public boolean readTag(ParkingTagData tag) {
		// If a null tag is passed in, then do nothing and return false.
		if (tag == null) {
			return false;
		}
		
		// If there are no more lines to be read, return false.
		String line = null;
		try {
			line = this.parkingTicketsReader.readLine();
		} catch (IOException ioe) {
			return false;
		}
		if (line == null) {
			return false;
		}
		
		// If shouldCaptureAllData is true, then split the line by , and fill
		// in all the tag data. Otherwise, only fill in the specific fields
		// we care about for the contest and ignore the rest.
		if (this.shouldCaptureAllData) {
			String[] fields = line.split(",");
			
			tag.tag_number = fields[0];
			tag.date_of_infraction = fields[1];
			tag.infraction_code = fields[2];
			tag.infraction_description = fields[3];
			tag.set_fine_amount = fields[4];
			tag.time_of_infraction = fields[5];
			tag.location1 = fields[6];
			tag.location2 = fields[7];
			tag.location3 = fields[8];
			tag.location4 = fields[9];
			tag.province = fields[10];
		} else {
			// The only fields we care about for the contest are set_fine_amount and
			// location2. start and end indicate the start and end indices of
			// interesting parts of the line. They're initialized to 18 to start because
			// the first two fields (tag_number_masked and date_of_infraction) have fixed
			// lengths and we don't need them, so we'll skip directly passed them.
			
			int start = 18;
			int end = 18;
			
			// Find the range of the set_fine_amount field. We're starting from the
			// third field (infraction_code), so we'll scan past two commas to reach the
			// fifth field (set_fine_amount).
			start = line.indexOf(',', start) + 1;
			start = line.indexOf(',', start) + 1;
			end = line.indexOf(',', start);
			
			tag.set_fine_amount = line.substring(start, end);
			
			// Find the range of the location2 field. It's another three fields in.
			start = line.indexOf(',', end) + 1;
			start = line.indexOf(',', start) + 1;
			start = line.indexOf(',', start) + 1;
			end = line.indexOf(',', start);
			
			tag.location2 = line.substring(start, end);
		}
		
		return true;
	}

}
