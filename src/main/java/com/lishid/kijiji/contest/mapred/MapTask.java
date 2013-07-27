package com.lishid.kijiji.contest.mapred;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.lishid.kijiji.contest.mapred.Algorithm.MapResult;
import com.lishid.kijiji.contest.util.CharArrayReader;
import com.lishid.kijiji.contest.util.PseudoString;

public class MapTask extends MapReduceTask {
    private static ThreadLocal<MapperResultCollector> perThreadResultCollector = new ThreadLocal<MapperResultCollector>();
    private ConcurrentLinkedQueue<char[]> recycler;
    private CharArrayReader dataReader;
    private MapperResultCollector resultCollector;
    
    public MapTask(TaskTracker taskTracker, ConcurrentLinkedQueue<char[]> recycler, CharArrayReader dataReader, int partitions) {
        super(taskTracker);
        this.recycler = recycler;
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
        PseudoString line;
        while ((line = dataReader.readLine()) != null) {
            try {
                Algorithm.map(line, mapResult);
                PseudoString key = mapResult.key;
                int value = mapResult.value;
                
                localResultCollector.collect(key.toString(), value);
            }
            catch (Exception e) {
                // Ignore bad lines
            }
        }
        recycler.offer(dataReader.getBuffer());
    }
    
    public static class MapperResultCollector {
        public List<HashMap<String, Integer>> partitionedResult;
        private int partitions;
        
        public MapperResultCollector(int partitions) {
            this.partitions = partitions;
        }
        
        public void init() {
            partitionedResult = new ArrayList<HashMap<String, Integer>>(partitions);
            for (int i = 0; i < partitions; i++) {
                partitionedResult.add(new HashMap<String, Integer>());
            }
        }
        
        public void collect(String key, int value) {
            int partition = getPartition(key.hashCode());
            Algorithm.combine(key, value, partitionedResult.get(partition));
        }
        
        private int getPartition(int hashCode) {
            return ((hashCode % partitions) + partitions) % partitions;
        }
    }
}
