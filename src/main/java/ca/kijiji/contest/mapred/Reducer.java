package ca.kijiji.contest.mapred;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import ca.kijiji.contest.TicketStatsCalculations;
import ca.kijiji.contest.mapred.Mapper.MapperResult;

public class Reducer extends MapReduceTask
{
    List<MapperResult> mapperResults;
    int reducerNumber;
    ReducerResult reducerResult;
    
    public Reducer(TaskTracker taskTracker, List<MapperResult> mapperResults, int reducerNumber)
    {
        super(taskTracker);
        this.mapperResults = mapperResults;
        this.reducerNumber = reducerNumber;
        reducerResult = new ReducerResult();
    }
    
    public ReducerResult getFutureResult()
    {
        return reducerResult;
    }
    
    @Override
    public void performTask()
    {
        // Reducer phase 1: Reduce data
        reducerResult.unsortedResult = null;
        for (MapperResult mapperResult : mapperResults)
        {
            if (reducerResult.unsortedResult == null)
            {
                // Use the first one as the unsorted map to save some time and memory
                reducerResult.unsortedResult = mapperResult.result.get(reducerNumber);
            }
            else
            {
                TicketStatsCalculations.reduce(mapperResult.result.get(reducerNumber), reducerResult.unsortedResult);
            }
        }
        // Reducer phase 2: Sort data
        reducerResult.result = TicketStatsCalculations.sort(reducerResult.unsortedResult);
    }
    
    public static class ReducerResult
    {
        Map<String, Integer> unsortedResult;
        SortedMap<String, Integer> result;
    }
}
