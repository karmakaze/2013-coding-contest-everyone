package ca.kijiji.contest.mapred;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import ca.kijiji.contest.IParkingTicketsStatsProcessor;
import ca.kijiji.contest.mapred.Mapper.MapperResult;
import ca.kijiji.contest.mapred.Reducer.ReducerResult;

public class MapReduceProcessor implements IParkingTicketsStatsProcessor
{
    // Tweakable variables
    private static final int AVAILABLE_CORES = Runtime.getRuntime().availableProcessors();
    private static final int MAPPER_LINES = 20000;
    
    private static long startTime;
    private ExecutorService executorService;
    
    public SortedMap<String, Integer> sortStreetsByProfitability(InputStream inputStream) throws Exception
    {
        executorService = Executors.newFixedThreadPool(AVAILABLE_CORES);
        
        startTime = System.currentTimeMillis();
        
        List<MapperResult> mapperResults = mapData(inputStream);
        List<ReducerResult> reducerResults = reduceData(mapperResults);
        
        Map<String, Integer> unsortedResult = reducerResults.get(0).unsortedResult;
        SortedMap<String, Integer> result = reducerResults.get(0).result;
        
        for (int i = 1; i < reducerResults.size(); i++)
        {
            unsortedResult.putAll(reducerResults.get(i).unsortedResult);
            result.putAll(reducerResults.get(i).result);
        }
        
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
        
        // Cleanup
        executorService.shutdown();
        
        return result;
    }
    
    private List<MapperResult> mapData(InputStream inputStream) throws Exception
    {
        // Create mapper tasks
        TaskTracker taskTracker = new TaskTracker(executorService);
        List<MapperResult> results = new ArrayList<MapperResult>();
        
        // Read data and dispatch
        // Start reading. IO is slow anyway and can't really multithread it.
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
        // Throw away the header
        reader.readLine();
        // Partition the data into MAPPER_LINES lines
        String[] buffer = new String[MAPPER_LINES];
        int index = 0;
        String line = null;
        while ((line = reader.readLine()) != null)
        {
            buffer[index] = line;
            index++;
            
            // Partition it out
            if (index == MAPPER_LINES)
            {
                results.add(startMapper(taskTracker, buffer));
                // Reset buffer
                buffer = new String[MAPPER_LINES];
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
        taskTracker.waitForTasks();
        
        printTime("Mappers all complete: ");
        return results;
    }
    
    private List<ReducerResult> reduceData(List<MapperResult> input) throws Exception
    {
        TaskTracker taskTracker = new TaskTracker(executorService);
        List<ReducerResult> results = new ArrayList<ReducerResult>(AVAILABLE_CORES);
        // Start running tasks
        for (int i = 0; i < AVAILABLE_CORES; i++)
        {
            results.add(startReducer(taskTracker, input, i));
        }
        // Wait until tasks are done
        taskTracker.waitForTasks();
        printTime("Reducers all complete: ");
        return results;
    }
    
    private MapperResult startMapper(TaskTracker taskTracker, String[] data)
    {
        Mapper mapper = new Mapper(taskTracker, data, AVAILABLE_CORES);
        taskTracker.startTask(mapper);
        return mapper.getResult();
    }
    
    private ReducerResult startReducer(TaskTracker taskTracker, List<MapperResult> mapperResults, int reducerNumber)
    {
        Reducer reducer = new Reducer(taskTracker, mapperResults, reducerNumber);
        taskTracker.startTask(reducer);
        return reducer.getResult();
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
