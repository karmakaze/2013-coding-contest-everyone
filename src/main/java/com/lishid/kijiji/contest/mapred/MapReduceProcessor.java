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
    
    /**
     * This implementation uses a technique similar to "MapReduce" to parallelize work by first performing independent
     * Map operations, then independent Reduce operations, and finally merging the result. <br>
     * More information on each step are located at {@link MapTask#performTask()} and {@link ReduceTask#performTask()} <br>
     * <br>
     * An optimization technique used here is avoiding the use of String operations as much as possible since it
     * creates temporary char[], which can be optimized by reusing the same char[] the buffered reading process used.
     */
    public SortedMap<String, Integer> sortStreetsByProfitability(InputStream inputStream) throws Exception {
        
        TaskTracker taskTracker = new TaskTracker(Executors.newFixedThreadPool(AVAILABLE_CORES));
        
        List<MapperResultCollector> mapperResults = map(taskTracker, inputStream);
        List<ReducerResultCollector> reducerResults = reduce(taskTracker, mapperResults);
        
        taskTracker.shutdown();
        
        SortedMap<String, Integer> result = merge(reducerResults);
        
        // File out = new File("C:\\Users\\lishid\\Desktop\\output.csv");
        // out.createNewFile();
        // PrintStream outPrintStream = new PrintStream(out);
        // for (Entry<String, Integer> road : result.entrySet()) {
        // outPrintStream.println(road.getKey() + ": " + road.getValue());
        // }
        // outPrintStream.close();
        // System.out.println(result.size());
        
        return result;
    }
    
    private List<MapperResultCollector> map(TaskTracker taskTracker, InputStream inputStream) throws Exception {
        List<MapperResultCollector> resultCollectors = new ArrayList<MapperResultCollector>();
        // Side note here, the inputstream contains 228304949 bytes, 228304515 chars. The file seems to be in ASCII though
        
        // Read the stream in large chunks. Individual line splitting will be done
        // on worker threads so as to parallelize as much work as possible
        LargeChunkReader reader = new LargeChunkReader(new InputStreamAsciiReader(inputStream));
        long startTime = System.currentTimeMillis();
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
        System.out.println("IO Took: " + (System.currentTimeMillis() - startTime));
        
        taskTracker.waitForTasksAndReset();
        
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
        SortedMap<String, Integer> sortedResult = null;
        
        for (int i = 1; i < input.size(); i++) {
            if (sortedResult == null) {
                sortedResult = new ParkingTicketTreeMap(input.get(i).result);
            }
            else {
                sortedResult.putAll(input.get(i).result);
            }
        }
        
        return sortedResult;
    }
}
