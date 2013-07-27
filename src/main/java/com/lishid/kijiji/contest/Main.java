package com.lishid.kijiji.contest;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.SortedMap;

import ca.kijiji.contest.ParkingTicketsStats;

public class Main {
    public static void main(String[] args) throws Exception {
        // For profilers
        System.in.read();
        
        int totalTime = 0;
        int runTimes = 10;
        for (int i = 0; i < runTimes; i++) {
            long startTime = System.currentTimeMillis();
            InputStream parkingTicketsStream = new FileInputStream("D:\\dropbox\\projects\\workspace\\kijiji\\2013-coding-contest\\src\\test\\resources\\Parking_Tags_Data_2012.csv");
            SortedMap<String, Integer> streets = ParkingTicketsStats.sortStreetsByProfitability(parkingTicketsStream);
            
            long duration = System.currentTimeMillis() - startTime;
            totalTime += duration;
            System.out.println("Duration of computation = " + duration + " ms");
            System.out.println(streets.get("KING"));
            System.out.println(streets.get("ST CLAIR"));
            System.out.println(streets.get(streets.firstKey()));
        }
        
        System.out.println("Average time: " + (totalTime / runTimes));
    }
}
