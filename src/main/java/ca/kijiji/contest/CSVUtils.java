package ca.kijiji.contest;

import java.io.IOException;
import java.io.StringReader;

import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

// Something to handle those 20 or so lines in the CSV with escaped columns
public class CSVUtils {

    private CSVUtils() { }

    /**
     * Parse a CSV-formatted line
     * @param csvLine a CSV-formatted line
     * @return columns parsed from csvLine
     */
    public static String[] parseCSVLine(String csvLine) {

        // My implementation was a bit faster, but this is less painful to read
        CsvListReader csvReader = new CsvListReader(new StringReader(csvLine), CsvPreference.STANDARD_PREFERENCE);
        String[] csvCols = new String[] {""};
        try {
            csvCols = csvReader.read().toArray(csvCols);
        } catch(IOException e) {
            // We can't have an IOException because there's no IO happening in a StringReader
        }

        return csvCols;
    }
}
