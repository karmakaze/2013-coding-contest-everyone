package ca.kijiji.contest;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Internal class for keeping track of profit per street between workers
 */
class StreetProfitMap extends ConcurrentHashMap<String, AtomicInteger> {
    /**
     * Add to the profit total for streetName
     * @param streetName street the infraction occurred on
     * @param fine fine to add to the street's total profit
     */
    public void addFineTo(String streetName, int fine) {
        // Look for the map entry for this street's profits
        AtomicInteger profitTracker = get(streetName);

        // Alright, couldn't find an existing profit tracker. We can't avoid locking now. Try putting one in,
        // or if someone else puts one in before us, use that.
        if(profitTracker == null) {
            final AtomicInteger newProfitTracker = new AtomicInteger(0);
            profitTracker = putIfAbsent(streetName, newProfitTracker);

            // Nobody tried inserting one before we did, use the one we just inserted.
            if(profitTracker == null) {
                profitTracker = newProfitTracker;
            }
        }

        // Add it to the total for this street
        profitTracker.getAndAdd(fine);
    }
}
