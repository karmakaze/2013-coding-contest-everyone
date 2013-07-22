package ca.kijiji.contest.mapred;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import ca.kijiji.contest.CommonCalculations;
import ca.kijiji.contest.CommonCalculations.MapResult;
import ca.kijiji.contest.util.CharArrayReader;

public class MapTask extends MapReduceTask {
    private static ThreadLocal<MapperResultCollector> perThreadResultCollector = new ThreadLocal<MapperResultCollector>();
    private char[] data;
    private int dataSize;
    private MapperResultCollector resultCollector;
    
    public MapTask(TaskTracker taskTracker, char[] data, int dataSize, int partitions) {
        super(taskTracker);
        this.data = data;
        this.dataSize = dataSize;
        resultCollector = new MapperResultCollector(partitions);
    }
    
    public MapperResultCollector getFutureResult() {
        return resultCollector;
    }
    
    /**
     * The map task: <br>
     * Split a large char array into lines and process each line individually. <br>
     * Each line is split into the key (street name) and value (ticket amount). <br>
     * The map task finally collects the result into a result collector (also partitions and combines)
     */
    @Override
    public void performTask() throws Exception {
        if (perThreadResultCollector.get() == null) {
            resultCollector.init();
            perThreadResultCollector.set(resultCollector);
        }
        // Use a per-thread collector instead of a per-mapper collector to better combine values
        MapperResultCollector localResultCollector = perThreadResultCollector.get();
        
        MapResult mapResult = new MapResult();
        CharArrayReader reader = new CharArrayReader(data, 0, dataSize);
        String line;
        while ((line = reader.readLine()) != null) {
            // Try catch to skip bad lines
            try {
                // Map
                CommonCalculations.map(line, mapResult);
                String key = mapResult.key;
                int value = mapResult.value;
                
                localResultCollector.collect(key, value);
            }
            catch (Exception e) {}
        }
    }
    
    public static class MapperResultCollector {
        public List<HashMap<String, Integer>> partitionedResult;
        private int partitions;
        
        public MapperResultCollector(int partitions) {
            this.partitions = partitions;
        }
        
        public void init() {
            partitionedResult = new ArrayList<HashMap<String, Integer>>();
            for (int i = 0; i < partitions; i++) {
                partitionedResult.add(new HashMap<String, Integer>());
            }
        }
        
        public void collect(String key, int value) {
            // Partition
            int partition = CommonCalculations.getPartition(key.hashCode(), partitions);
            // Combine
            CommonCalculations.combine(key, value, partitionedResult.get(partition));
        }
    }
}
