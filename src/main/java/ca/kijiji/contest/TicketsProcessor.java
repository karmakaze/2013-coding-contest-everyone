package ca.kijiji.contest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * @author Sergey Bushkov
 */
public class TicketsProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(TicketsProcessor.class);
    private final static String[] STREET_TYPES = {
            "AV", "AVE", "AVENUE", "BL", "BLVD",
            "CIR", "CR", "CRCL", "CRCT", "CRES", "CRT", "CT",
            "D", "DR", "GDN", "GDNS", "LA", "LANE", "PARKWAY", "PK", "PKWY", "PL",
            "RAOD", "RD", "ROAD", "S", "ST", "STREET", "WAY"};
    private final static String[] DIRECTIONS = {
            "E", "EAST", "N", "NORTH", "S", "SOUTH", "W", "WEST"};
    private final Pattern STREET_NAME_WORD = Pattern.compile("[A-Z'\\-]+"); // "KING", "ST", "CLAIRE", "D'ARCY"
    private final Pattern STREET_NAME_NUMBER = Pattern.compile("[0-9]+(ST|RD|TH)"); // "12TH", "43RD"
    private final Pattern TOKEN_SEPARATOR = Pattern.compile("[ ,.]+"); // spaces, double spaces, extra punctuation, etc.
    private final Map<String, Counter> streetsProfit = new HashMap<>();

    static {
        // make sure arrays are sorted - need it for binary search
        Arrays.sort(STREET_TYPES);
        Arrays.sort(DIRECTIONS);
    }

    /*
     * Extracts the fine amount and street name from the CSV string; then updates the street profit map.
     *
     * Tickets with zero fine amount, and tickets with empty "location2" field are ignored.
     *
     * The street name is obtained in the following way:
     * 1) location2 is split into tokens (words);
     * 2) direction and street type tokens are removed from the end;
     * 3) starting from the end of the remaining tokens list, the sequence of words ("KING", "ST", "CLAIRE", "D'ARCY")
     * and number-like names ("12TH", "43RD", but not "1A", "1a") is combined into street name.
     * 4) the rest is considered as number(s)/range, and ignored.
     */
    public void processTicketRecord(String ticketRecord) {
        String[] fields = splitCSV(ticketRecord);
        if (fields.length != 11) {
            LOG.error("Problem with parsing CSV: " + ticketRecord);
            return;
        }
        int fineAmount;
        try {
            fineAmount = Integer.parseInt(fields[4]);
        } catch (NumberFormatException e) {
            LOG.error("Bad line (fine amount is not an integer number): " + ticketRecord);
            return;
        }
        if (fineAmount <= 0) {
            return;
        }
        String location2 = fields[7];
        if (location2.isEmpty()) {
            return;
        }

        String[] tokens = TOKEN_SEPARATOR.split(location2, 0);
        int lastIndex = tokens.length - 1;
        if (lastIndex > 0 && Arrays.binarySearch(DIRECTIONS, tokens[lastIndex]) >= 0) {
            lastIndex--;
        }
        if (lastIndex > 0 && Arrays.binarySearch(STREET_TYPES, tokens[lastIndex]) >= 0) {
            lastIndex--;
        }
        String streetName = "";
        while (lastIndex >= 0 && isValidStreetNameWord(tokens[lastIndex])) {
            if (streetName.length() > 0) {
                streetName = tokens[lastIndex] + " " + streetName;
            } else {
                streetName = tokens[lastIndex];
            }
            lastIndex--;
        }
        addStreetProfit(streetName, fineAmount);
    }

    /*
     * Simple CSV string parser - works for all strings in the test tickets file, including quoted values with commas.
     * Double quotes and other funny CSV features are not supported.
     */
    private String[] splitCSV(String csvString) {
        if (csvString.indexOf('"') < 0) {
            return csvString.split(",", -1);
        } else {
            ArrayList<String> tokens = new ArrayList<>(11);
            StringBuilder sb = new StringBuilder(32);
            boolean quoted = false;
            for (int i = 0, len = csvString.length(); i < len; i++) {
                char c = csvString.charAt(i);
                if (c == '"') {
                    if (!quoted && sb.length() == 0) {
                        quoted = true;
                    } else if (quoted) {
                        quoted = false;
                    } else {
                        sb.append(c); // quote in the middle of unquoted string - 121 L"AMOREAUX DR
                    }
                } else if (c == ',' && !quoted) {
                    tokens.add(sb.toString());
                    sb.setLength(0);
                } else {
                    sb.append(c);
                }
            }
            tokens.add(sb.toString());
            return tokens.toArray(new String[tokens.size()]);
        }
    }

    private boolean isValidStreetNameWord(String word) {
        return STREET_NAME_WORD.matcher(word).matches() || STREET_NAME_NUMBER.matcher(word).matches();
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
