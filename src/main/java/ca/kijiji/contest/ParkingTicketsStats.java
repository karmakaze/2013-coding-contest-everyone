package ca.kijiji.contest;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.kijiji.contest.enums.Direction;
import ca.kijiji.contest.enums.Suffix;
import ca.kijiji.contest.utils.ValueComparableMap;

import com.google.common.collect.Ordering;

/**
 * Processes parking fine data to arrange the street by revenue earned.
 * 
 * @author Vinayak Hulawale
 * 
 */
public class ParkingTicketsStats {

	private static final String SPACE = " ";
	private static final String COMMA = ",";
	private static final Logger LOG = LoggerFactory
			.getLogger(ParkingTicketsStats.class);

	/*
	 * Map to cache the extracted street names
	 */
	private static Map<String, String> streetNameMap = new HashMap<String, String>();

	public static SortedMap<String, Integer> sortStreetsByProfitability(
			InputStream parkingTicketsStream) {

		Map<String, Integer> keySortedMap = getStreetToFineAmountMap(parkingTicketsStream);

		/**
		 * Sort the map based on cumulative fine amount.
		 */
		ValueComparableMap<String, Integer> valueSortedMap = new ValueComparableMap<>(
				Ordering.natural().reverse()); // comparator to sort the value
												// in desc. order
		valueSortedMap.putAll(keySortedMap);

		return valueSortedMap;

	}

	/**
	 * Reads the file and stores the data in hash map
	 * 
	 * @param parkingTicketsStream
	 * @return map of street names to fine amount.
	 */
	private static Map<String, Integer> getStreetToFineAmountMap(
			InputStream parkingTicketsStream) {

		Map<String, Integer> allParkingTkts = new HashMap<String, Integer>(100);

		BufferedReader reader = new BufferedReader(new InputStreamReader(
				parkingTicketsStream));
		try {
			// ignore header from file
			reader.readLine();
			// process data from file line by line.
			for (String parkingTktLine = reader.readLine(); parkingTktLine != null; parkingTktLine = reader
					.readLine()) {
				String[] parkingTktData = StringUtils.splitPreserveAllTokens(
						parkingTktLine, COMMA);
				if (parkingTktData.length != 11) {
					// ignore bad records
					continue;
				}
				String streetName = getStreetName(parkingTktData[7]);
				// Assuming fine is always integer value
				Integer oldValue = allParkingTkts.put(streetName, new Integer(
						parkingTktData[4]));
				// if street is already added, add the current fine amount and
				// insert again
				if (oldValue != null) {
					allParkingTkts.put(streetName, oldValue
							+ new Integer(parkingTktData[4]));
				}

			}

		} catch (Exception exp) {
			LOG.error("Can not read file", exp);
		}

		return allParkingTkts;
	}

	/**
	 * Extract the street name from address field.
	 * 
	 * @param streetAddress
	 * @return
	 */
	private static String getStreetName(final String streetAddress) {
		String streetName = null;
		streetName = streetNameMap.get(streetAddress);
		// if street name is already extracted use it from map else extract
		if (streetName == null) {
			StringBuilder sbStreetName = new StringBuilder();
			String[] locationArray = StringUtils.splitPreserveAllTokens(
					streetAddress, SPACE);
			int locationArrayLength = locationArray.length;
			if (locationArrayLength == 1) {
				// Assuming this to be street name
				streetName = streetAddress;
			} else {
				for (int i = 0; i < locationArrayLength; i++) {
					// ignoring first numeric field in input
					if (i == 0 && StringUtils.isNumeric(locationArray[i])) {
						continue;
					} else if (isValidStreetName(locationArray[i], i,
							sbStreetName.length())) {
						sbStreetName.append(locationArray[i] + SPACE);
					}

				}
				streetName = sbStreetName.toString().trim();
				// cache for future use
				streetNameMap.put(streetAddress, streetName);
			}

		}
		return streetName;
	}

	/**
	 * Evaluate current fragment is valid street name or not.
	 * 
	 * @param locationPart
	 * @param position
	 * @param bufferLength
	 * @return
	 */
	private static boolean isValidStreetName(String locationPart, int position,
			int bufferLength) {

		// first or second fragment is always valid given current street name is
		// empty.
		if ((position == 0 || position == 1) && bufferLength == 0) {
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