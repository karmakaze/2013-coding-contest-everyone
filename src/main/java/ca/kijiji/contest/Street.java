package ca.kijiji.contest;

import java.io.Serializable;

/**
 * Simple Street data object, holding street name and profit 
 * with the standard getters and setters and a method for  
 * adding another street's profit to this street's profit.
 * Two constructors are given: one with parameters and a copy constructor
 * @author Chiara
 */
public final class Street implements Serializable {
    private static final long serialVersionUID = -192560734138299510L;
    private String name;
    private int profit;

    public Street(String n, int p) {
        setName(n);
        setProfit(p);
    }

    public Street(Street o) {
	setName(o.name);
	setProfit(o.getProfit());
    }
    
    
    public int getProfit() {
        return profit;
    }

    public void setProfit(int profit) {
        this.profit = profit;
    }

    public void addProfit(Street o) {
	this.profit += o.profit;
    }
    
    public void setName(String name) {
	this.name = name;
    }
    
    public String getName() {
	return name;
    }
}