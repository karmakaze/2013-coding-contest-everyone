package ca.kijiji.contest.jfuerth;

import java.util.Map;

public final class StreetAmount implements Map.Entry<String, Integer>
{
    
    private int amount;
    private final String street;
    
    public StreetAmount(int amount, String location)
    {
        this.amount = amount;
        
        if (location == null)
        {
            throw new NullPointerException();
        }
        this.street = location;
    }
    
    public void add(int amount)
    {
        this.amount += amount;
    }
    
    @Override
    public String toString()
    {
        return street + ": $" + amount;
    }
    
    // --- Map.Entry Implementation ---
    
    public String getKey()
    {
        return street;
    }
    
    public Integer getValue()
    {
        return amount;
    }
    
    public Integer setValue(Integer value)
    {
        throw new UnsupportedOperationException();
    }
}