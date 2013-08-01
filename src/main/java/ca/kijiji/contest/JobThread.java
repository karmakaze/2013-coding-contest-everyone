package ca.kijiji.contest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** parses the given line of the csv file. Then adds the profit to the streets map */
public class JobThread implements Runnable {
    private String line;

    private static final Logger LOG = LoggerFactory.getLogger(JobThread.class);

    public JobThread(String line){
        this.line = line;
    }

    @Override
    public void run() {
        try {
            process();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void process() throws Exception {
        String[] tokens = line.split(",");

        // clean-up price
        String price = tokens[4]; // set_fine_amount
        price = price.trim();

        // clean-up street
        String streetOrig = tokens[7]; // location 2
        String street = streetOrig.toUpperCase().trim();
        street = ParkingTicketsStats.cleanupStreetName(street);

        // add profit to running total
        Integer tmp = ParkingTicketsStats.streets.get(street);
        if (tmp == null) {
            ParkingTicketsStats.streets.put(street, Integer.valueOf(price));
        } else {
            ParkingTicketsStats.streets.put(street, Integer.valueOf(price) + tmp);
        }
    }
}