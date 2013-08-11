package ca.kijiji.contest;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Simple street name matcher - @see https://github.com/karmakaze/2013-coding-contest
 *
 * @author Sergey Bushkov;
 */
public class TicketsProcessor {
    private static final Pattern namePattern = Pattern.compile("([A-Z][A-Z][A-Z]+|ST [A-Z][A-Z][A-Z]+)");
    private final Matcher nameMatcher = namePattern.matcher("");
    private final Map<String, Counter> streetsProfit = new HashMap<>();

    /*
     * Extracts the fine amount and street name from the CSV string; then updates the street profit map.
     * Tickets with zero fine amount are ignored.
     *
     * Using simple street name matcher - @see https://github.com/karmakaze/2013-coding-contest
     */
    public void processTicketRecord(String ticketRecord) {
        int start = 0;
        int end = 0;
        for (int i = 0; i < 5; i++) {
            start = end;
            end = ticketRecord.indexOf(',', end + 1);
        }
        int fineAmount = Integer.parseInt(ticketRecord.substring(start + 1, end));
        if (fineAmount <= 0) {
            return;
        }
        for (int i = 0; i < 3; i++) {
            start = end;
            end = ticketRecord.indexOf(',', end + 1);
        }
        String location2 = ticketRecord.substring(start + 1, end);
        if (nameMatcher.reset(location2).find()) {
            String streetName = nameMatcher.group();
            addStreetProfit(streetName, fineAmount);
        }
    }

    private void addStreetProfit(String street, int profit) {
        Counter counter = streetsProfit.get(street);
        if (counter != null) {
            counter.add(profit);
        } else {
            streetsProfit.put(street, new Counter(profit));
        }
    }

    public void mergeResult(TicketsProcessor anotherProcessor) {
        for (Map.Entry<String, Counter> entry : anotherProcessor.streetsProfit.entrySet()) {
            addStreetProfit(entry.getKey(), entry.getValue().getValue());
        }
    }

    public Map<String, Integer> getResult() {
        Map<String, Integer> result = new HashMap<>();
        for (Map.Entry<String, Counter> entry : streetsProfit.entrySet()) {
            result.put(entry.getKey(), entry.getValue().getValue());
        }
        return result;
    }

    static class Counter {
        private int value;

        Counter(int initialValue) {
            value = initialValue;
        }

        void add(int amount) {
            value += amount;
        }

        int getValue() {
            return value;
        }
    }
}
