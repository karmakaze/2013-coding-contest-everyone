package com.lishid.kijiji.contest.mapred;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.lishid.kijiji.contest.CommonCalculations;
import com.lishid.kijiji.contest.CommonCalculations.MapResult;
import com.lishid.kijiji.contest.util.CharArrayReader;

public class MapTask extends MapReduceTask {
    private static ThreadLocal<MapperResultCollector> perThreadResultCollector = new ThreadLocal<MapperResultCollector>();
    private CharArrayReader dataReader;
    private MapperResultCollector resultCollector;
    
    public MapTask(TaskTracker taskTracker, CharArrayReader dataReader, int partitions) {
        super(taskTracker);
        this.dataReader = dataReader;
        resultCollector = new MapperResultCollector(partitions);
    }
    
    public MapperResultCollector getFutureResult() {
        return resultCollector;
    }
    
    /**
     * The map task: <br>
     * Split a large char array into lines and process each line individually. <br>
     * Each line is split into the key (street name, processed and cleaned) and value (ticket amount). <br>
     * The map task finally collects the result into a result collector (also partitions and combines)
     */
    @Override
    public void performTask() throws Exception {
        // Use a per-thread collector instead of a per-mapper collector to better combine values
        if (perThreadResultCollector.get() == null) {
            resultCollector.init();
            perThreadResultCollector.set(resultCollector);
        }
        MapperResultCollector localResultCollector = perThreadResultCollector.get();
        
        MapResult mapResult = new MapResult();
        String line;
        while ((line = dataReader.readLine()) != null) {
            try {
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
            int partition = CommonCalculations.getPartition(key.hashCode(), partitions);
            CommonCalculations.combine(key, value, partitionedResult.get(partition));
        }
    }
}
