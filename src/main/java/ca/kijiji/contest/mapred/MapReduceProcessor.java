package ca.kijiji.contest.mapred;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.Executors;

import ca.kijiji.contest.mapred.MapTask.MapperResultCollector;
import ca.kijiji.contest.mapred.ReduceTask.ReducerResultCollector;
import ca.kijiji.contest.util.CharArrayReader;
import ca.kijiji.contest.util.LargeChunkReader;

public class MapReduceProcessor {
    
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
        
        // Read the stream in large chunks. Individual line splitting will be done
        // on worker threads so as to parallelize as much work as possible
        LargeChunkReader reader = new LargeChunkReader(new InputStreamReader(inputStream));
        int read = 1;
        while (read > 0) {
            char[] buffer = new char[MAPPER_CHUNK_SIZE];
            read = reader.readChunk(buffer);
            if (read > 0) {
                CharArrayReader charArrayReader = new CharArrayReader(buffer, 0, read);
                MapTask task = new MapTask(taskTracker, charArrayReader, PARTITIONS);
                taskTracker.startTask(task);
                resultCollectors.add(task.getFutureResult());
            }
        }
        reader.close();
        
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
        List<ReducerResultCollector> resultCollectors = new ArrayList<ReducerResultCollector>(AVAILABLE_CORES);
        
        for (int i = 0; i < PARTITIONS; i++) {
            ReduceTask task = new ReduceTask(taskTracker, input, i);
            taskTracker.startTask(task);
            resultCollectors.add(task.getFutureResult());
        }
        
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
