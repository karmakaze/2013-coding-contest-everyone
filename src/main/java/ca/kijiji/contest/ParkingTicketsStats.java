package ca.kijiji.contest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.kijiji.contest.enums.Direction;
import ca.kijiji.contest.enums.Suffix;
import ca.kijiji.contest.utils.ValueComparableMap;

import com.google.common.collect.Ordering;

/**
 * 
 * @author Vinayak Hulawale
 *
 */
public class ParkingTicketsStats {

	private static final String SPACE = " ";
	private static final String SPACE_REGEX = "\\s+";
	private static final String COMMA = ",";
	private static final Logger LOG = LoggerFactory
			.getLogger(ParkingTicketsStats.class);

	public static SortedMap<String, Integer> sortStreetsByProfitability(
			InputStream parkingTicketsStream) {

		SortedMap<String, Integer> keySortedMap = getStreetToFineAmountMap(parkingTicketsStream);
		
		ValueComparableMap<String,Integer> valueSortedMap = new ValueComparableMap<>(Ordering.natural().reverse());
		valueSortedMap.putAll(keySortedMap);
		
		return valueSortedMap;

	}

	/**
	 * Reads the file and stores the data in value sorted map which gets sorted on the fly 
	 * @param parkingTicketsStream
	 * @return
	 */
	private static SortedMap<String, Integer> getStreetToFineAmountMap(
			InputStream parkingTicketsStream) {

		TreeMap<String, Integer> allParkingTkts = new TreeMap<String, Integer>(
				);
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				parkingTicketsStream));
		try {
			// ignore header from file
			reader.readLine();
			for (String parkingTktLine = reader.readLine(); parkingTktLine != null; parkingTktLine = reader
					.readLine()) {
				String[] parkingTktData = parkingTktLine.split(COMMA);
				if (parkingTktData.length != 11) {
					// ignore bad records
					continue;
				}
				String streetName = getStreetName(parkingTktData[7]);
				if (allParkingTkts.containsKey(streetName)) {
					allParkingTkts.put(
							streetName,
							allParkingTkts.get(streetName)
									+ Integer.parseInt(parkingTktData[4]));
				} else {
					allParkingTkts.put(streetName,
							Integer.parseInt(parkingTktData[4]));
				}

			}

		} catch (IOException exp) {
			LOG.error("Can not read file", exp);
		}

		return allParkingTkts;
	}

	private static String getStreetName(final String streetAddress) {
		String streetName = null;
		if (streetAddress != null) {
			StringBuilder sbStreetName = new StringBuilder();
			String[] locationArray = streetAddress.split(SPACE_REGEX);
			int locationArrayLength = locationArray.length;
			if (locationArrayLength == 1) {
				// Assuming this to be street name
				streetName = streetAddress;
			} else {
				for (int i = 0; i < locationArrayLength; i++) {
					// ignoring first numeric field in input
					if (i == 0 && StringUtils.isNumeric(locationArray[i])) {
						continue;
					} else if (isValidStreetName(locationArray[i], i)) {
						sbStreetName.append(locationArray[i] + SPACE);
					}

				}
			}
			streetName = sbStreetName.toString().trim();
		}
		return streetName;
	}

	private static boolean isValidStreetName(String locationPart, int position) {

		if ((position == 0 || position == 1)) {
			return true;
		}

		// Ignore suffix and direction
		if (Direction.isValidDirection(locationPart)
				|| Suffix.isValidSuffix(locationPart)) {
			return false;
		}

		return true;
	}

}