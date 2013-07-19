package ca.kijiji.contest.simple;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.SortedMap;

import ca.kijiji.contest.IParkingTicketsStatsProcessor;
import ca.kijiji.contest.TicketStatsCalculations;
import ca.kijiji.contest.TicketStatsCalculations.LineMappingResult;

public class SingleThreadedProcessor implements IParkingTicketsStatsProcessor
{
    static long startTime;
    
    public SortedMap<String, Integer> sortStreetsByProfitability(InputStream inputStream) throws Exception
    {
        startTime = System.currentTimeMillis();
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
        // Throw away the header
        reader.readLine();
        String[] keys = new String[2746154];
        int[] values = new int[2746154];
        String line = null;
        int index = 0;
        LineMappingResult lineMappingResult = new LineMappingResult();
        while ((line = reader.readLine()) != null)
        {
            TicketStatsCalculations.map(line, lineMappingResult);
            keys[index] = lineMappingResult.roadName;
            values[index] = lineMappingResult.amount;
            index++;
        }
        reader.close();
        printTime("Mapping complete: ");
        HashMap<String, Integer> result = new HashMap<String, Integer>();
        for (int i = 0; i < keys.length; i++)
        {
            TicketStatsCalculations.combine(keys[i], values[i], result);
        }
        
        SortedMap<String, Integer> resultMap = TicketStatsCalculations.sort(result);
        printTime("Reducing complete: ");
        
        return resultMap;
    }
    
    public static void printTime(String text)
    {
        long newTime = System.currentTimeMillis();
        System.out.println(text + (newTime - startTime));
        startTime = newTime;
    }
}
