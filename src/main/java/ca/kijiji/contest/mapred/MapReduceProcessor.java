package ca.kijiji.contest.mapred;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.Executors;

import ca.kijiji.contest.IParkingTicketsStatsProcessor;
import ca.kijiji.contest.mapred.MapTask.MapperResultCollector;
import ca.kijiji.contest.mapred.ReduceTask.ReducerResultCollector;
import ca.kijiji.contest.util.LargeChunkReader;

public class MapReduceProcessor implements IParkingTicketsStatsProcessor {
    /*
     * == Checklist ==
     * Readability: JavaDoc, in-code commenting, conventional naming, class/method separation, OOP-design
     * Efficiency: Tested average <1800ms on an Intel Core i7-3520M, 8GB system memory, 7200RPM Momentus Thin
     * - History: 3800ms -> 2500ms -> 2000ms -> 1900ms -> 1850ms -> 1800ms -> 1750ms
     * - Optimizations:
     * - - Multi-threading :D
     * - - MapReduce model
     * - - Combiner at mapper side to pre-reduce, saves memory
     * - - Moved line splitting (Originally from BufferedReader) to mappers for more parallel processing
     * - - Common calculations optimizations, omitting some accuracy as it's not the main goal
     * - - Chunk size values should be played around to obtain the best value. This might depend on the Disk specs
     * Parallel Processing: Using MapReduce to parallelize tasks that are independent of each other
     * Creativity: I think using the MapReduce model is kinda interesting as it's generally used for much larger data
     * using distributed systems
     */
    
    /*
     * == Memo ==
     * - Estimate of what takes time
     * - 1. IO read (So far, the largest factor. Determined by computer specs. Taking more than half the total time for processing)
     * - 2. Splitting the lines (Minimal but seems to be an improvement when moved to multithreaded line splitting)
     * - 3. Analysing the lines to get the road name and ticket amount (Minimize cases to process faster since this operation apply to every line)
     * - 4. Adding everything up (Will be done once per record anyway, so parallelize using the reduce step seems a good choice)
     * - 5. Sorting (Reduce tasks tree-sort it, then main thread merge the sorted trees together)
     */
    
    // Tweakable variables
    private static final int AVAILABLE_CORES = Runtime.getRuntime().availableProcessors();
    private static int MAPPER_CHUNK_SIZE = 1 << 18; // 524288 (seemed like the best value on my setup)
    private static int PARTITIONS = AVAILABLE_CORES;
    
    /**
     * This implementation uses the "Map Reduce" technique to parallelize work by first performing independent
     * Map operations, then independent Reduce operations, and finally merging the result. <br>
     * More information on each step are located at {@link MapTask#performTask()} and {@link ReduceTask#performTask()}
     */
    public SortedMap<String, Integer> sortStreetsByProfitability(InputStream inputStream) throws Exception {
        // Create a TaskTracker to run and track MapReduceTasks
        TaskTracker taskTracker = new TaskTracker(Executors.newFixedThreadPool(AVAILABLE_CORES));
        
        // Map!
        List<MapperResultCollector> mapperResults = map(taskTracker, inputStream);
        // Reduce!
        List<ReducerResultCollector> reducerResults = reduce(taskTracker, mapperResults);
        // Kill unnecessary threads
        taskTracker.shutdown();
        // Merge!
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
        
        // Read the stream in large chunks. Line splitting will be done on worker threads so as to parallelize work
        LargeChunkReader reader = new LargeChunkReader(new InputStreamReader(inputStream));
        int charsRead = 1;
        while (charsRead > 0) {
            char[] buffer = new char[MAPPER_CHUNK_SIZE];
            // Using the custom chunk reader, we can squeeze some performance time from splitting lines to
            // parallelize that work into multiple threads.
            // Also save some memory since there is no need to create temporary String objects
            charsRead = reader.readChunk(buffer);
            if (charsRead > 0) {
                MapTask task = new MapTask(taskTracker, buffer, charsRead, PARTITIONS);
                taskTracker.startTask(task);
                resultCollectors.add(task.getFutureResult());
            }
        }
        reader.close();
        
        // Wait until tasks are done
        taskTracker.waitForTasksAndReset();
        return resultCollectors;
    }
    
    private List<ReducerResultCollector> reduce(TaskTracker taskTracker, List<MapperResultCollector> input) throws Exception {
        List<ReducerResultCollector> resultCollectors = new ArrayList<ReducerResultCollector>(AVAILABLE_CORES);
        
        for (int i = 0; i < PARTITIONS; i++) {
            ReduceTask task = new ReduceTask(taskTracker, input, i);
            taskTracker.startTask(task);
            resultCollectors.add(task.getFutureResult());
        }
        
        // Wait until tasks are done
        taskTracker.waitForTasksAndReset();
        return resultCollectors;
    }
    
    private SortedMap<String, Integer> merge(List<ReducerResultCollector> input) {
        Map<String, Integer> unsortedResult = input.get(0).unsortedResult;
        SortedMap<String, Integer> result = input.get(0).result;
        
        for (int i = 1; i < input.size(); i++) {
            unsortedResult.putAll(input.get(i).unsortedResult);
            result.putAll(input.get(i).result);
        }
        
        return result;
    }
    
}
