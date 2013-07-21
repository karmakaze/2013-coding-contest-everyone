package ca.kijiji.contest.exceptions;

public class UnparseableLocationException extends Exception {

	private static final long serialVersionUID = -4561227676709678003L;

	private String _location = null;
	
	public UnparseableLocationException(String location) {
		this._location = location;
	}
	
	public String toString() {
		return String.format("Unable to parse location: %s", this._location);
	}
	
	public String getLocation() {
		return this._location;
	}
}
