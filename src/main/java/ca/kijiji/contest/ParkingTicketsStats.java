package ca.kijiji.contest;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParkingTicketsStats {

    public static SortedMap<String, Integer> sortStreetsByProfitability(InputStream parkingTicketsStream) {

        BufferedReader br = null;
    	String line = "";

    	String street = null;
    	Integer fine_amount = null;

    	Pattern p = Pattern.compile("^(\\d+?) (.*?) {0,1}(ST|STREET|AV|AVE|COURT|CRT|CT|RD){0,1} {0,1}(N|S|E|W|EAST|WEST|NORTH|SOUTH){0,1}$");
    	HashMap<String, Integer> map = new HashMap<String, Integer>();
    	ValueComparator vc = new ValueComparator(map);
    	SortedMap<String, Integer> fines = new TreeMap<String, Integer>(vc);

    	try {
    		br = new BufferedReader(new InputStreamReader(parkingTicketsStream));
    		while ((line = br.readLine()) != null) {

    			String[] row = line.split(",");

    			Matcher m = p.matcher(row[7]);
    			while (m.find()) {
    				street = m.group(2).trim();
    				fine_amount = Integer.parseInt(row[4]);
    				map.put(street, map.containsKey(street) ? map.get(street) + fine_amount : fine_amount);
    			}

    		}
    	} catch (FileNotFoundException e) {
    		e.printStackTrace();
    	} catch (IOException e) {
    		e.printStackTrace();
    	} finally {
    		if (br != null) {
    			try {
    				br.close();
    			} catch (IOException e) {
    				e.printStackTrace();
    			}
    		}
    	}

    	fines.putAll(map);

    	return fines;

    }

    static class ValueComparator implements Comparator<String> {

        Map<String, Integer> base;

        ValueComparator(Map<String, Integer> base) {
            this.base = base;
        }

        public int compare(String a, String b) {
            Integer x = base.get(b);
            Integer y = base.get(a);
            if (x.equals(y)) {
                return a.compareTo(b);
            }
            return x.compareTo(y);
        }

    }

}
