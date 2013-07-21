package ca.kijiji.contest.mapred;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import ca.kijiji.contest.TicketStatsCalculations;
import ca.kijiji.contest.TicketStatsCalculations.MapResult;

public class Mapper extends MapReduceTask {
    private static ThreadLocal<MapperResultCollector> perThreadResultCollector = new ThreadLocal<MapperResultCollector>();
    private String[] data;
    private MapperResultCollector resultCollector;
    
    public Mapper(TaskTracker taskTracker, String[] data, int partitions) {
        super(taskTracker);
        this.data = data;
        resultCollector = new MapperResultCollector(partitions);
    }
    
    public MapperResultCollector getFutureResult() {
        return resultCollector;
    }
    
    @Override
    public void performTask() {
        if (perThreadResultCollector.get() == null) {
            resultCollector.init();
            perThreadResultCollector.set(resultCollector);
        }
        
        MapperResultCollector localResultCollector = perThreadResultCollector.get();
        
        MapResult lineMappingResult = new MapResult();
        // Process each line
        for (int i = 0; i < data.length; i++) {
            String line = data[i];
            
            if (line == null) {
                break;
            }
            
            // Map
            TicketStatsCalculations.map(line, lineMappingResult);
            
            String key = lineMappingResult.key;
            int value = lineMappingResult.value;
            
            localResultCollector.collect(key, value);
        }
    }
    
    public static class MapperResultCollector {
        List<HashMap<String, Integer>> partitionedResult;
        int partitions;
        
        public MapperResultCollector(int partitions) {
            this.partitions = partitions;
        }
        
        public void init() {
            this.partitionedResult = new ArrayList<HashMap<String, Integer>>();
            for (int i = 0; i < partitions; i++) {
                partitionedResult.add(new HashMap<String, Integer>());
            }
        }
        
        public void collect(String key, int value) {
            // Partition
            int partition = TicketStatsCalculations.getPartition(key.hashCode(), partitions);
            
            // Combine
            TicketStatsCalculations.combine(key, value, partitionedResult.get(partition));
        }
    }
}
