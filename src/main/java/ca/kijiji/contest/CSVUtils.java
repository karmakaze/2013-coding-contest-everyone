package ca.kijiji.contest;

import java.io.*;

import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

// Something to handle those 20 or so lines in the CSV with escaped columns
class CSVUtils {

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

        return csvCols;
    }
}
