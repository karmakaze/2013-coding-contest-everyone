package ca.burnison.kijiji.contest;


/**
 * Encapsulates all that is tickety.
 */
public final class Ticket
{
    private final String street;

    /**
     * It so happens that the source data is in integer dollars. Should the source be a rational number, a tough
     * decision must be made: accuracy vs. efficiency. In the case of accuracy, a BigDecimal should be used, whereas in
     * the case of efficiency at the cost of minor accuracy, a DoubleAdder * can be used. This is a business decision.
     * Alternatively, if a fixed-base currency is used, it's reasonable that the value can be parsed as an integer and
     * then divided when being read.
     */
    private final int amount;

    /**
     * @param street Normalized street name.
     * @param amount Ticket amount in dollars.
     */
    public Ticket(final String street, final int amount)
    {
        this.street = street;
        this.amount = amount;
    }

    public String getStreet()
    {
        return this.street;
    }

    public int getAmount()
    {
        return this.amount;
    }
}
