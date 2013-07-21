package ca.kijiji.contest.exceptions;

public class UnparseableLocationException extends Exception {

	private static final long serialVersionUID = -4561227676709678003L;

	private String location = null;
	
	public UnparseableLocationException(String location) {
		this.location = location;
	}
	
	public String toString() {
		return String.format("Unable to parse location: %s", this.location);
	}
}
