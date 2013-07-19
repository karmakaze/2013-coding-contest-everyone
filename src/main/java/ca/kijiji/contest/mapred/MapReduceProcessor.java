package ca.kijiji.contest.mapred;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.Executors;

import ca.kijiji.contest.IParkingTicketsStatsProcessor;
import ca.kijiji.contest.mapred.Mapper.MapperResult;
import ca.kijiji.contest.mapred.Reducer.ReducerResult;

public class MapReduceProcessor implements IParkingTicketsStatsProcessor
{
    // Tweakable variables
    private static final int AVAILABLE_CORES = Runtime.getRuntime().availableProcessors();
    private static int MAPPER_CHUNK_SIZE = 20000;
    
    // Debugging info
    private static long startTime;
    
    public SortedMap<String, Integer> sortStreetsByProfitability(InputStream inputStream) throws Exception
    {
        startTime = System.currentTimeMillis();
        // Create a TaskTracker to run and track MapReduceTasks
        TaskTracker taskTracker = new TaskTracker(Executors.newFixedThreadPool(AVAILABLE_CORES));
        
        // Map!
        List<MapperResult> mapperResults = mapData(taskTracker, inputStream);
        printTime("Mappers completed: ");
        // Reduce!
        List<ReducerResult> reducerResults = reduceData(taskTracker, mapperResults);
        printTime("Reducers completed: ");
        // Kill unnecessary threads
        taskTracker.shutdown();
        
        // Final data merging
        Map<String, Integer> unsortedResult = reducerResults.get(0).unsortedResult;
        SortedMap<String, Integer> result = reducerResults.get(0).result;
        
        for (int i = 1; i < reducerResults.size(); i++)
        {
            unsortedResult.putAll(reducerResults.get(i).unsortedResult);
            result.putAll(reducerResults.get(i).result);
        }
        printTime("Final merge completed: ");
        
        // File out = new File("C:\\Users\\lishid\\Desktop\\output.csv");
        // out.createNewFile();
        // PrintStream outPrintStream = new PrintStream(out);
        // for (Entry<String, Integer> road : result.entrySet())
        // {
        // outPrintStream.println(road.getKey() + ": " + road.getValue());
        // }
        // outPrintStream.close();
        // System.out.println(result.size());
        
        // printMemory();
        
        return result;
    }
    
    private List<MapperResult> mapData(TaskTracker taskTracker, InputStream inputStream) throws Exception
    {
        List<MapperResult> results = new ArrayList<MapperResult>();
        
        // Start reading. IO is slow anyway and can't really multithread it.
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
        // Throw away the header
        reader.readLine();
        // Partition the data into MAPPER_CHUNK_SIZE lines
        String[] buffer = new String[MAPPER_CHUNK_SIZE];
        int index = 0;
        String line = null;
        while ((line = reader.readLine()) != null)
        {
            buffer[index] = line;
            index++;
            
            // Partition it out TODO
            if (index == MAPPER_CHUNK_SIZE)
            {
                results.add(startMapper(taskTracker, buffer));
                // Reset buffer
                if (taskTracker.startedTasks - taskTracker.finishedTasks < AVAILABLE_CORES)
                {
                    MAPPER_CHUNK_SIZE = 2000;
                }
                else
                {
                    MAPPER_CHUNK_SIZE = 20000;
                }
                buffer = new String[MAPPER_CHUNK_SIZE];
                index = 0;
            }
        }
        reader.close();
        // Clear out the last partition if necessary
        if (index > 0)
        {
            results.add(startMapper(taskTracker, buffer));
            buffer = null;
        }
        printTime("Mappers dispatch complete: ");
        // Wait until tasks are done
        taskTracker.waitForTasksAndReset();
        return results;
    }
    
    private List<ReducerResult> reduceData(TaskTracker taskTracker, List<MapperResult> input) throws Exception
    {
        List<ReducerResult> results = new ArrayList<ReducerResult>(AVAILABLE_CORES);
        // Start tasks
        for (int i = 0; i < AVAILABLE_CORES; i++)
        {
            results.add(startReducer(taskTracker, input, i));
        }
        printTime("Reducers dispatch complete: ");
        // Wait until tasks are done
        taskTracker.waitForTasksAndReset();
        return results;
    }
    
    private MapperResult startMapper(TaskTracker taskTracker, String[] data)
    {
        Mapper mapper = new Mapper(taskTracker, data, AVAILABLE_CORES);
        taskTracker.startTask(mapper);
        return mapper.getFutureResult();
    }
    
    private ReducerResult startReducer(TaskTracker taskTracker, List<MapperResult> mapperResults, int reducerNumber)
    {
        Reducer reducer = new Reducer(taskTracker, mapperResults, reducerNumber);
        taskTracker.startTask(reducer);
        return reducer.getFutureResult();
    }
    
    // Util
    
    public static void printMemory()
    {
        System.gc();
        
        int mb = 1024 * 1024;
        
        // Getting the runtime reference from system
        Runtime runtime = Runtime.getRuntime();
        
        System.out.println("##### Heap utilization statistics [MB] #####");
        
        // Print used memory
        System.out.println("Used Memory:" + (runtime.totalMemory() - runtime.freeMemory()) / mb);
        
        // Print free memory
        System.out.println("Free Memory:" + runtime.freeMemory() / mb);
        
        // Print total available memory
        System.out.println("Total Memory:" + runtime.totalMemory() / mb);
        
        // Print Maximum available memory
        System.out.println("Max Memory:" + runtime.maxMemory() / mb);
    }
    
    public static void printTime(String text)
    {
        long newTime = System.currentTimeMillis();
        System.out.println(text + (newTime - startTime));
        startTime = newTime;
    }
}
