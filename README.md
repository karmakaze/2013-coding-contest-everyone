So here's my solution, seeing that everyone else is posting theirs.

Core
------

The key concept is the use of MapReduce, which separates the task into two distinctive stages that run concurrently and without synchronization.

Most of the other solutions out there requires synchronization when the result is collected (such as using a single ConcurrentHashMap to collect results), whereas the MapReduce process uses an array of 4 HashMap (per "map" thread) each collecting specific values (string.hashcode mod 4), then 4 ("reduce") threads take one of the item in the array of 4 HashMaps and combine them together. This means that each thread uses its own memory space and no synchronization is necessary, thus high performance.

Reading the input in large chunks and splitting it fairly often (so threads don't go idle) is necessary. Line splitting is done on individual threads to reduce work on the IO thread as much as possible. I've seen this in many of the solutions posted on Github.

Other Optimizations
------

One thing I've noticed is that String.split() is very slow. This is mostly because of copying the char[] many times and creating the new String objects.

As an optimization, MutableString is created to reduce the overhead of String creation and manipulation (such as .split(), .substring, etc). 

This seem to be the ultimate optimization for this solution as it cuts down the (back then, already multithreaded and otherwise optimized) processing time by more than half.

Another optimization is in the address parsing. Since accuracy is not very important, all this solution does is strip of non-alphabetic tokens at the beginning and filtered suffixes at the end. The beginning of a MutableString can be easily read char by char to determine if a word is fully alphabetic. The tokens after can be checked against a HashSet to determine if they should be filtered.

Testing
------

My solution runs for about 500-550 ms (first run, using JUnit test) and 400ms (multi-run, usually 10, java hotspot-optimized)

This is tested on a Intel i7-3520M (Dual-core with HT = 4 threads) at 2.9GHz, 8GB RAM but only 1G for Java.

IO wise, the data file is stored on a 7200-RPM Momentus Thin HDD, and seems to take about 100ms to fully read the file without storing the data nor processing it.

The "Map" phase takes about 300-350ms, which is the total of IO, running 4 threads to crunch the ~2.7million lines of data.

The "Reduce" phase takes less than 100ms, which is mostly just merging the results from map.

The final merging and sorting phase is done on the main thread, and takes about 50-100ms.

Possible optimizations
------

An optimization would be to implement a custom SortedMap that can do a customized merge-sort. The plan is to sort the data on 4 threads, then merge them together into one on the main thread. Doing so will parallelize most of the final sorting phase, and the only unparallized work is the final merge phase.

