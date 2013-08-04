Here is my solution.

Overview
--------

The input stream is processed using one reader thread and several processor threads.

The reader thread reads strings from the input stream, then puts packets of data to the working queue. Using packets of data (1000 strings) helps to avoid excessive working queue locking. When the end of the stream is reached, end markers are added to the queue.

Processor threads take data packets from the working queue. The data goes to the actual parser associated with each processor thread. The processor thread stops when it reads an end marker from the working queue.

Each processor creates its own street->profit map; which helps to avoid locks on profit maps. In the end, the data is merged (similar to MapReduce), and the SortedMap view is returned.


Ticket processing
-----------------

Parsing CSV line: for most of the records, String.split() worked fine; but some records contain fields with comma inside (such fields are wrapped with quotes). This makes line parser more complicated.

Getting street names:
- location2 is split into tokens (words);
- direction and street type tokens are removed from the end;
- starting from the end of the remaining tokens list, the sequence of words ("KING", "ST", "CLAIRE", "D'ARCY"), and number-like names ("12TH", "43RD", but not "1A", "1a") is combined into street name.
- the remaining part of the location string rest is considered as number(s)/range, and ignored.

The String.split() calls and regexps are slow; so there is a room for optimization. I had no time to optimize it though; and also tried to focus on getting different street names right (=avoid premature optimization). Then, decided to submit the "readable" solution as is.
