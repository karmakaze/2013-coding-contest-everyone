package ca.kijiji.contest.enums;

/**
 * 
 * @author Vinayak Hulawale
 *
 */
public enum Suffix {

	ST, STREET, AV, AVE, COURT, CRT, CT, RD;
	
	/**
	 * Checks whether given suffix is valid or not
	 * @param direction
	 * @return
	 */
	public static boolean isValidSuffix(String suffix) {
		
		for(Suffix suffixEnum : values()) {
			if(suffixEnum.toString().equalsIgnoreCase(suffix)){
				return true;
			}
		}
		return false;
	}
}
