package com.lishid.kijiji.contest.mapred;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;

import com.lishid.kijiji.contest.mapred.MapTask.MapperResultCollector;
import com.lishid.kijiji.contest.mapred.ReduceTask.ReducerResultCollector;
import com.lishid.kijiji.contest.util.CharArrayReader;
import com.lishid.kijiji.contest.util.InputStreamAsciiReader;
import com.lishid.kijiji.contest.util.LargeChunkReader;
import com.lishid.kijiji.contest.util.ParkingTicketTreeMap;

public class MapReduceProcessor {
    
    // Tweakable variables
    private static final int AVAILABLE_CORES = Runtime.getRuntime().availableProcessors();
    private static final int MAPPER_CHUNK_SIZE = 1 << 20; // 2097152 (seemed like the best value on my setup)
    private static final int PARTITIONS = AVAILABLE_CORES;
    
    /** Recycle those large char arrays using a thread-safe object pool */
    private static final ConcurrentLinkedQueue<char[]> recycler = new ConcurrentLinkedQueue<char[]>();
    long startTime = System.currentTimeMillis();
    
    /**
     * This implementation uses a technique similar to "MapReduce" to parallelize work by first performing independent
     * Map operations, then independent Reduce operations, and finally merging the result. <br>
     * More information on each step are located at {@link MapTask#performTask()} and {@link ReduceTask#performTask()}
     */
    public SortedMap<String, Integer> sortStreetsByProfitability(InputStream inputStream) throws Exception {
        TaskTracker taskTracker = new TaskTracker(Executors.newFixedThreadPool(AVAILABLE_CORES));
        List<MapperResultCollector> mapperResults = map(taskTracker, inputStream);
        List<ReducerResultCollector> reducerResults = reduce(taskTracker, mapperResults);
        
        taskTracker.shutdown();
        
        SortedMap<String, Integer> result = merge(reducerResults);
        
        return result;
    }
    
    private List<MapperResultCollector> map(TaskTracker taskTracker, InputStream inputStream) throws Exception {
        startTime = System.currentTimeMillis();
        List<MapperResultCollector> resultCollectors = new ArrayList<MapperResultCollector>();
        // Side note here, the inputstream contains 228304949 bytes, 228304515 chars
        
        // Read the stream in large chunks. Individual line splitting will be done
        // on worker threads so as to parallelize as much work as possible
        LargeChunkReader reader = new LargeChunkReader(new InputStreamAsciiReader(inputStream));
        int read = 1;
        while (read > 0) {
            char[] buffer = recycler.poll();
            if (buffer == null) {
                buffer = new char[MAPPER_CHUNK_SIZE];
            }
            read = reader.readChunk(buffer);
            if (read > 0) {
                CharArrayReader charArrayReader = new CharArrayReader(buffer, 0, read);
                MapTask task = new MapTask(taskTracker, recycler, charArrayReader, PARTITIONS);
                taskTracker.startTask(task);
                resultCollectors.add(task.getFutureResult());
            }
            else {
                recycler.offer(buffer);
            }
        }
        reader.close();
        
        System.out.println("Map dispatched " + (System.currentTimeMillis() - startTime));
        taskTracker.waitForTasksAndReset();
        System.out.println("Map done " + (System.currentTimeMillis() - startTime));
        // System.out.println(recycler.size());
        
        List<MapperResultCollector> validResults = new ArrayList<MapperResultCollector>();
        for (MapperResultCollector collector : resultCollectors) {
            if (collector.partitionedResult != null) {
                validResults.add(collector);
            }
        }
        return validResults;
    }
    
    private List<ReducerResultCollector> reduce(TaskTracker taskTracker, List<MapperResultCollector> input) throws Exception {
        List<ReducerResultCollector> resultCollectors = new ArrayList<ReducerResultCollector>(PARTITIONS);
        
        for (int i = 0; i < PARTITIONS; i++) {
            ReduceTask task = new ReduceTask(taskTracker, input, i);
            taskTracker.startTask(task);
            resultCollectors.add(task.getFutureResult());
        }
        
        taskTracker.waitForTasksAndReset();
        return resultCollectors;
    }
    
    private SortedMap<String, Integer> merge(List<ReducerResultCollector> input) {
        SortedMap<String, Integer> sortedResult = new ParkingTicketTreeMap(input.get(0).result);
        
        for (int i = 1; i < input.size(); i++) {
            sortedResult.putAll(input.get(i).result);
        }
        
        return sortedResult;
    }
}
