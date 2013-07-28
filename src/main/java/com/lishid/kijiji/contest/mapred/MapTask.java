package com.lishid.kijiji.contest.mapred;

import java.util.HashMap;

import com.lishid.kijiji.contest.mapred.Algorithm.MapResult;
import com.lishid.kijiji.contest.util.ByteArrayReader;
import com.lishid.kijiji.contest.util.MutableInteger;
import com.lishid.kijiji.contest.util.MutableString;

public class MapTask extends MapReduceTask {
    private static ThreadLocal<MapperResultCollector> perThreadResultCollector = new ThreadLocal<MapperResultCollector>();
    private ByteArrayReader dataReader;
    private MapperResultCollector resultCollector;
    
    public MapTask(TaskTracker taskTracker, ByteArrayReader dataReader, int partitions) {
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
        MutableString line;
        MutableInteger recyclableInteger = null;
        while ((line = dataReader.readLine()) != null) {
            try {
                Algorithm.map(line, mapResult);
                MutableString key = mapResult.key;
                int value = mapResult.value;
                if (recyclableInteger == null) {
                    recyclableInteger = new MutableInteger(value);
                }
                else {
                    recyclableInteger.useAsNewInteger(value);
                }
                recyclableInteger = localResultCollector.collect(key, recyclableInteger);
            }
            catch (Exception e) {
                recyclableInteger = null;
                // Ignore bad lines
            }
        }
    }
    
    public static class MapperResultCollector {
        public MapperResultPartition[] partitionedResult;
        private int partitions;
        
        public MapperResultCollector(int partitions) {
            this.partitions = partitions;
        }
        
        public void init() {
            partitionedResult = new MapperResultPartition[partitions];
            for (int i = 0; i < partitions; i++) {
                partitionedResult[i] = new MapperResultPartition(16384);
            }
        }
        
        public MutableInteger collect(MutableString key, MutableInteger value) {
            int partition = getPartition(key.hashCode());
            return Algorithm.combine(key, value, partitionedResult[partition]);
        }
        
        private int getPartition(int input) {
            return Math.abs(input) % partitions;
        }
    }
    
    private static class MapperResultPartition extends HashMap<MutableString, MutableInteger> {
        private static final long serialVersionUID = 1L;
        
        public MapperResultPartition(int defaultSize) {
            super(defaultSize);
        }
    }
}
