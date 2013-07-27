package com.lishid.kijiji.contest.mapred;

import java.util.List;
import java.util.Map;

import com.lishid.kijiji.contest.mapred.MapTask.MapperResultCollector;

public class ReduceTask extends MapReduceTask {
    List<MapperResultCollector> mapperResults;
    int partition;
    ReducerResultCollector resultCollector;
    
    public ReduceTask(TaskTracker taskTracker, List<MapperResultCollector> mapperResults, int partition) {
        super(taskTracker);
        this.mapperResults = mapperResults;
        this.partition = partition;
        resultCollector = new ReducerResultCollector();
    }
    
    public ReducerResultCollector getFutureResult() {
        return resultCollector;
    }
    
    /**
     * The reducer task: <br>
     * Take all mapper results for the same reducer partition and reduce(combine) it. <br>
     */
    @Override
    public void performTask() throws Exception {
        resultCollector.result = null;
        for (MapperResultCollector mapperResult : mapperResults) {
            if (resultCollector.result == null) {
                resultCollector.result = mapperResult.partitionedResult.get(partition);
            }
            else {
                Algorithm.reduce(mapperResult.partitionedResult.get(partition), resultCollector.result);
            }
        }
    }
    
    public static class ReducerResultCollector {
        Map<String, Integer> result;
    }
}
