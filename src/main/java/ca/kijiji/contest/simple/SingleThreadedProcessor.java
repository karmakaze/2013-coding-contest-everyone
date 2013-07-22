package ca.kijiji.contest.simple;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.SortedMap;

import ca.kijiji.contest.IParkingTicketsStatsProcessor;
import ca.kijiji.contest.CommonCalculations;
import ca.kijiji.contest.CommonCalculations.MapResult;

public class SingleThreadedProcessor implements IParkingTicketsStatsProcessor {
    static long startTime;
    
    public SortedMap<String, Integer> sortStreetsByProfitability(InputStream inputStream) throws Exception {
        startTime = System.currentTimeMillis();
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
        // Throw away the header
        reader.readLine();
        String[] keys = new String[2746154];
        int[] values = new int[2746154];
        String line = null;
        int index = 0;
        MapResult lineMappingResult = new MapResult();
        while ((line = reader.readLine()) != null) {
            CommonCalculations.map(line, lineMappingResult);
            keys[index] = lineMappingResult.key;
            values[index] = lineMappingResult.value;
            index++;
        }
        reader.close();
        printTime("Mapping complete: ");
        HashMap<String, Integer> result = new HashMap<String, Integer>();
        for (int i = 0; i < keys.length; i++) {
            CommonCalculations.combine(keys[i], values[i], result);
        }
        
        SortedMap<String, Integer> resultMap = CommonCalculations.sort(result);
        printTime("Reducing complete: ");
        
        return resultMap;
    }
    
    public static void printTime(String text) {
        long newTime = System.currentTimeMillis();
        System.out.println(text + (newTime - startTime));
        startTime = newTime;
    }
}
