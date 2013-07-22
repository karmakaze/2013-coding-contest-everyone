package ca.kijiji.contest;

import java.util.ArrayList;

// Just for fun, something to handle those 20 or so lines in the CSV with escaped lines,
// doesn't handle escaped newlines since readLine() would destroy them and the CSV
// doesn't have any :)
public class CSVUtils {

    private CSVUtils() { }

    /**
     * Parse a pseudo-CSV formatted line
     * @param csvLine pseudo-CSV formatted line
     * @return Un-escaped columns parsed from csvLine
     */
    public static String[] parseCSVLine(String csvLine) {

        // The last field may end with a newline sequence, throw it out since we can't
        // handle them properly.
        if(csvLine.endsWith("\r\n")) {
            csvLine = csvLine.substring(0, csvLine.length() - 2);
        }

        int lineLen = csvLine.length();

        ArrayList<String> csvCols = new ArrayList<>();

        StringBuilder colBuilder = new StringBuilder();

        // Whether or not we're inside a quoted field
        boolean insideQuotes = false;

        // What index the current field starts at
        int fieldStart = 0;

        for(int i = 0; i < lineLen; ++i) {

            boolean skipChar = false;

            // Assume we'll never run into surrogate pairs (and we shouldn't) for speed.
            char currChar = csvLine.charAt(i);

            // Handle characters whose meaning depends on context
            switch(currChar) {

                case '"':
                    //We're inside a quoted field, check if this is an escaped quote or not
                    if(insideQuotes) {
                        // Where to check for a quote following this one
                        int nextQuoteCheck = i + 1;

                        // This is an un-escaped quote (end of line or not followed by another quote,)
                        // this ends the field.
                        if(nextQuoteCheck >= lineLen || csvLine.charAt(nextQuoteCheck) != '"') {
                            insideQuotes = false;
                            skipChar = true;
                        }
                        // This is an escaped quote. Print this one, but skip the following quote.
                        else {
                            i = nextQuoteCheck;
                        }
                    } else {
                        // We're not in a quoted field... is this quote at the start of the field?
                        if(i == fieldStart) {
                            // That means this is a quoted field.
                            insideQuotes = true;
                            // Skip past this quote
                            skipChar = true;
                        }

                        // If it isn't at the field start and this isn't a quoted field, we print the quote as-is.
                    }
                    break;

                case ',':
                    if(!insideQuotes) {
                        // This is a field-ending comma. Add the string-ified field to our list of fields.
                        csvCols.add(colBuilder.toString());
                        colBuilder = new StringBuilder();
                        skipChar = true;

                        // Set the start of the next field to just after this
                        fieldStart = i + 1;
                    }

                    // If we're inside the quotes for our column, treat commas like any other character
                    break;
            }

            if(!skipChar) {
                colBuilder.append(currChar);
            }
        }

        // Add the last column to the list (there's always at least one)
        csvCols.add(colBuilder.toString());

        return csvCols.toArray(new String[csvCols.size()]);
    }
}
