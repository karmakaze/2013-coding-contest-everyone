package ca.kijiji.contest;

import java.io.*;

import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

// Something to handle those 20 or so lines in the CSV with escaped columns
final class CSVUtils {

    private CSVUtils() { }

    /**
     * Parse a CSV-formatted line
     * @param csvLine a CSV-formatted line
     * @return columns parsed from csvLine
     */
    // My implementation was a bit faster, but this is less painful to read
    public static String[] parseCSVLine(String csvLine) {

        // Make a reader that can parse the line into a list of columns
        CsvListReader csvReader = new CsvListReader(new StringReader(csvLine), CsvPreference.STANDARD_PREFERENCE);

        // A row in a CSV always contains at least 1 empty column
        String[] csvCols = new String[] {""};
        try {
            // Parse the line and store the resultant columns
            csvCols = csvReader.read().toArray(csvCols);
        } catch(IOException e) {
            // We can't have an IOException because there's no IO happening in a StringReader
        }

        // SuperCSV ever-so-helpfully  converts empty fields to nulls, when we want empty strings
        // We can do this automatically with string processors *if* we know beforehand how many columns
        // there will be. I don't want to hardcode that, so... we do it this way.
        for(int i = 0; i < csvCols.length; ++i) {
            if(csvCols[i] == null) {
                csvCols[i] = "";
            }
        }

        return csvCols;
    }
}
