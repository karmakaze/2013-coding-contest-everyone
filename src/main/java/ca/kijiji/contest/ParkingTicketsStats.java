package ca.kijiji.contest;

import java.io.InputStream;
import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ParkingTicketsStats {
    public ConcurrentMap<String, Integer> streetProfitMap;
    public byte[] data;

    private InputStream stream;
    private StreetComparator streetComparator;
    private SortedMap<String, Integer> sortedStreetProfitMap;

    //Constructor
    public ParkingTicketsStats(InputStream iStream) {
        stream = iStream;
        streetProfitMap = new ConcurrentHashMap<String, Integer>();
    }

    public static SortedMap<String, Integer> sortStreetsByProfitability(InputStream parkingTicketsStream) {
        ParkingTicketsStats parkingTicketsStats = new ParkingTicketsStats(parkingTicketsStream);
        return parkingTicketsStats.calculate();
    }

    public SortedMap<String, Integer> sort(){
        streetComparator = new StreetComparator(streetProfitMap);
        sortedStreetProfitMap = new TreeMap<String, Integer>(streetComparator);
        sortedStreetProfitMap.putAll(streetProfitMap);
        return sortedStreetProfitMap;
    }

    public SortedMap<String, Integer> calculate(){
        readDataToMemory();
        parseData();
        return sort();
    }

    private void readDataToMemory(){
        try {
            data = new byte[stream.available()];
            stream.read(data);
        } catch (IOException ioException){

        }
    }

    private void parseData(){
        Thread[] threads = createThreads(4);
        startThreads(threads);
    }

    private void startThreads(Thread[] threads){
        //start jobs
        for (int i = 0; i < threads.length; i++){
            threads[i].start();
        }
        //wait for jobs to finish
        for (int i = 0; i < threads.length; i++){
            try {
                threads[i].join();
            } catch (InterruptedException interrupt) {

            }
        }
    }

    private Thread[] createThreads(int numberOfThreads) {
        Thread[] threads = new Thread[numberOfThreads];

        for (int i = 0; i < numberOfThreads; i++){
            threads[i] = new Thread(new WorkerThread(i, numberOfThreads, this));
        }

        return threads;
    }
}

