package ca.kijiji.contest.mapred;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import ca.kijiji.contest.TicketStatsCalculations;
import ca.kijiji.contest.TicketStatsCalculations.LineMappingResult;

public class Mapper extends MapReduceTask
{
    private String[] data;
    private MapperResult result;
    private int reducers;
    
    public Mapper(TaskTracker taskTracker, String[] data, int reducers)
    {
        super(taskTracker);
        this.data = data;
        this.reducers = reducers;
        result = new MapperResult();
    }
    
    public MapperResult getResult()
    {
        return result;
    }
    
    @Override
    public void perform()
    {
        // Initiate result storage
        for (int i = 0; i < reducers; i++)
        {
            result.result.add(new HashMap<String, Integer>());
        }
        LineMappingResult lineMappingResult = new LineMappingResult();
        // Process each line
        for (int i = 0; i < data.length; i++)
        {
            // Mapper phase 1, generating key-values
            String line = data[i];
            
            if (line == null)
            {
                break;
            }
            
            TicketStatsCalculations.mapLine(line, lineMappingResult);
            
            String key = lineMappingResult.roadName;
            int value = lineMappingResult.amount;
            
            // Mapper phase 2, reduce
            int reducer = ((key.hashCode() % reducers) + reducers) % reducers;
            TicketStatsCalculations.reduceData(key, value, result.result.get(reducer));
        }
    }
    
    public static class MapperResult
    {
        List<HashMap<String, Integer>> result = new ArrayList<HashMap<String, Integer>>();
    }
}
