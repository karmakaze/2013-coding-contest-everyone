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
    private int partitions;
    
    public Mapper(TaskTracker taskTracker, String[] data, int reducers)
    {
        super(taskTracker);
        this.data = data;
        this.partitions = reducers;
        result = new MapperResult();
    }
    
    public MapperResult getFutureResult()
    {
        return result;
    }
    
    @Override
    public void performTask()
    {
        // Initiate result storage
        for (int i = 0; i < partitions; i++)
        {
            result.result.add(new HashMap<String, Integer>());
        }
        LineMappingResult lineMappingResult = new LineMappingResult();
        // Process each line
        for (int i = 0; i < data.length; i++)
        {
            String line = data[i];
            
            if (line == null)
            {
                break;
            }

            // Map
            TicketStatsCalculations.map(line, lineMappingResult);
            String key = lineMappingResult.roadName;
            int value = lineMappingResult.amount;

            // Partition
            int partition = TicketStatsCalculations.getPartition(key.hashCode(), partitions);
            
            // Combine
            TicketStatsCalculations.combine(key, value, result.result.get(partition));
        }
    }
    
    public static class MapperResult
    {
        List<HashMap<String, Integer>> result = new ArrayList<HashMap<String, Integer>>();
    }
}
