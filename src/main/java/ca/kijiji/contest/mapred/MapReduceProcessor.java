package ca.kijiji.contest.mapred;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.concurrent.Executors;

import ca.kijiji.contest.IParkingTicketsStatsProcessor;
import ca.kijiji.contest.mapred.Mapper.MapperResultCollector;
import ca.kijiji.contest.mapred.Reducer.ReducerResultCollector;

public class MapReduceProcessor implements IParkingTicketsStatsProcessor {
    // Tweakable variables
    private static final int AVAILABLE_CORES = Runtime.getRuntime().availableProcessors();
    private static int MAPPER_CHUNK_SIZE = 20000;
    private static int PARTITIONS = AVAILABLE_CORES;
    
    // Debugging info
    private static long startTime;
    
    public SortedMap<String, Integer> sortStreetsByProfitability(InputStream inputStream) throws Exception {
        startTime = System.currentTimeMillis();
        // Create a TaskTracker to run and track MapReduceTasks
        TaskTracker taskTracker = new TaskTracker(Executors.newFixedThreadPool(AVAILABLE_CORES));
        
        // Map!
        List<MapperResultCollector> mapperResults = map(taskTracker, inputStream);
        printTime("Map completed: ");
        // Reduce!
        List<ReducerResultCollector> reducerResults = reduce(taskTracker, mapperResults);
        printTime("Reduce completed: ");
        // Kill unnecessary threads
        taskTracker.shutdown();
        
        // Final data merging
        Map<String, Integer> unsortedResult = reducerResults.get(0).unsortedResult;
        SortedMap<String, Integer> result = reducerResults.get(0).result;
        
        for (int i = 1; i < reducerResults.size(); i++) {
            unsortedResult.putAll(reducerResults.get(i).unsortedResult);
            result.putAll(reducerResults.get(i).result);
        }
        printTime("Final merge completed: ");
        
        File out = new File("C:\\Users\\lishid\\Desktop\\output.csv");
        out.createNewFile();
        PrintStream outPrintStream = new PrintStream(out);
        for (Entry<String, Integer> road : result.entrySet()) {
            outPrintStream.println(road.getKey() + ": " + road.getValue());
        }
        outPrintStream.close();
        System.out.println(result.size());
        
        return result;
    }
    
    private List<MapperResultCollector> map(TaskTracker taskTracker, InputStream inputStream) throws Exception {
        List<MapperResultCollector> results = new ArrayList<MapperResultCollector>();
        
        // Start reading. IO is slow anyway and can't really multithread it.
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
        // Throw away the header
        reader.readLine();
        // Partition the data into MAPPER_CHUNK_SIZE lines
        String[] buffer = new String[MAPPER_CHUNK_SIZE];
        int index = 0;
        String line = null;
        while ((line = reader.readLine()) != null) {
            buffer[index] = line;
            index++;
            
            if (index == MAPPER_CHUNK_SIZE) {
                results.add(startMapper(taskTracker, buffer));
                // Reset buffer
                buffer = new String[MAPPER_CHUNK_SIZE];
                index = 0;
            }
        }
        reader.close();
        // Clear out the last partition if necessary
        if (index > 0) {
            results.add(startMapper(taskTracker, buffer));
            buffer = null;
        }
        printTime("Mappers dispatch complete: ");
        // Wait until tasks are done
        taskTracker.waitForTasksAndReset();
        return results;
    }
    
    private List<ReducerResultCollector> reduce(TaskTracker taskTracker, List<MapperResultCollector> input) throws Exception {
        List<ReducerResultCollector> results = new ArrayList<ReducerResultCollector>(AVAILABLE_CORES);
        // Start tasks
        for (int i = 0; i < PARTITIONS; i++) {
            results.add(startReducer(taskTracker, input, i));
        }
        printTime("Reducers dispatch complete: ");
        // Wait until tasks are done
        taskTracker.waitForTasksAndReset();
        return results;
    }
    
    private MapperResultCollector startMapper(TaskTracker taskTracker, String[] data) {
        Mapper mapper = new Mapper(taskTracker, data, PARTITIONS);
        taskTracker.startTask(mapper);
        return mapper.getFutureResult();
    }
    
    private ReducerResultCollector startReducer(TaskTracker taskTracker, List<MapperResultCollector> mapperResults, int partition) {
        Reducer reducer = new Reducer(taskTracker, mapperResults, partition);
        taskTracker.startTask(reducer);
        return reducer.getFutureResult();
    }
    
    // Util
    
    public static void printTime(String text) {
        long newTime = System.currentTimeMillis();
        System.out.println(text + (newTime - startTime));
        startTime = newTime;
    }
}
