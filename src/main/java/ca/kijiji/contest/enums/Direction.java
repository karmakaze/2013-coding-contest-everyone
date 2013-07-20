package ca.kijiji.contest.enums;

/**
 * 
 * @author Vinayak Hulawale
 *
 */
public enum Direction {

	EAST, WEST, E, W, N, S;
	
	/**
	 * Checks whether given direction is valid or not
	 * @param direction
	 * @return
	 */
	public static boolean isValidDirection(String direction) {
		
		for(Direction directionEnum : values()) {
			if(directionEnum.toString().equalsIgnoreCase(direction)){
				return true;
			}
		}
		return false;
	}
}
