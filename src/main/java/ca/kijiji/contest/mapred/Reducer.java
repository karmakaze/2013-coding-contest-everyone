package ca.kijiji.contest.mapred;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import ca.kijiji.contest.TicketStatsCalculations;
import ca.kijiji.contest.mapred.Mapper.MapperResultCollector;

public class Reducer extends MapReduceTask {
    List<MapperResultCollector> mapperResults;
    int partition;
    ReducerResultCollector resultCollector;
    
    public Reducer(TaskTracker taskTracker, List<MapperResultCollector> mapperResults, int partition) {
        super(taskTracker);
        this.mapperResults = mapperResults;
        this.partition = partition;
        resultCollector = new ReducerResultCollector();
    }
    
    public ReducerResultCollector getFutureResult() {
        return resultCollector;
    }
    
    @Override
    public void performTask() {
        // Reducer phase 1: Reduce data
        resultCollector.unsortedResult = null;
        for (MapperResultCollector mapperResult : mapperResults) {
            if (mapperResult.partitionedResult == null) {
                continue;
            }
            if (resultCollector.unsortedResult == null) {
                // Use the first one as the unsorted map to save some time and memory
                resultCollector.unsortedResult = mapperResult.partitionedResult.get(partition);
            }
            else {
                TicketStatsCalculations.reduce(mapperResult.partitionedResult.get(partition), resultCollector.unsortedResult);
            }
        }
        // Reducer phase 2: Sort data
        resultCollector.result = TicketStatsCalculations.sort(resultCollector.unsortedResult);
    }
    
    public static class ReducerResultCollector {
        Map<String, Integer> unsortedResult;
        SortedMap<String, Integer> result;
    }
}
