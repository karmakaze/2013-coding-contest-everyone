So yes, I think I can code :-) And I hope you'll agree with me :-)

This file explains a few things about my solution...

OVERVIEW

The solution is achieved in three steps:
- first, an additional dataset containing "clean" street names is loaded. This dataset has been
  obtained from a different source of the Toronto municipality, the Toronto Centerline dataset:
  
  http://www1.toronto.ca/wps/portal/contentonly?vgnextoid=9acb5f9cd70bb210VgnVCM1000003dd60f89RCRD&vgnextchannel=1a66e03bb8d1e310VgnVCM10000071d60f89RCRD
  
  I opened the dataset, extracted only the relevant information (street names),  
  removed suffixes and duplicates. The resulting file is in /src/resources/centerline_clean.txt
  Being a non-native english speaker, I might have overlooked a few things in this cleaning (I could
  have removed suffixes that are in fact part of the street name, but I also read your comment on the
  blog so I decided not to worry too much :-)
  In case a different file should be tested, the NAME OF THE FILE can be configured in the options (see below) 
  
- second, the parking tickets datafile (i.e. the inputstream) is read in large byte chunks, whose SIZE can
  also be configured in the options. For each chunk, a new thread is spawned to process the data.
  The number of spawned threads is the size of the datafile/the size of the data chunks, rounded up.
  
  Each of these threads extracts the location and amount fields, cleans a bit the location field and then tries to
  match each extracted street name to a "clean" street name, i.e. a street name that is in the dataset 
  parsed at the previous step. 
  In order to determine if an extracted street name matches a clean street name, the algorithm imposes that
  the first three characters be equal in both strings, then applies a MEASURE OF SIMILARITY to the remainder 
  of the string. The match is the clean street name that 
      - begins with the same three letters
      - has the maximum value of similarity to the target string with respect to all of the other "clean" street names
      - has a similarity above a THRESHOLD.  

  If no match can be found, the extracted street data is discarded.
  When all streets are matched and profits summed, the result is merged by each thread to a common dictionary, that 
  holds the unsorted street names and the profits
  
  The measure of similarity can be chosen among two: one. called "local", favours strings with higher correspondance in
  the first characters, with a low penalization for dissimilarities in the last part of the string. This accounts
  for the fact that most suffixes are not removed from the street names extracted from the datafile, while it is
  expected that the first part of street names has a higher probability of being written correctly (it's the same
  assumption that lead to impose that the first three chars must match). However, insertions or deletions that happen early
  in the string are highly penalized. 
  The second measure of similarity is calculated using a standard distance metric, the DamerauLevenshtein, that
  allows for constant penalization of insertions, deletions, substitutions and swaps. 
  
  Both measures seem to work fine, even considering that the clean street names do not contain the suffix and the 
  extracted ones do, so I prefer to use the local one because it's faster.  
  
- finally, a new SortedMap that sorts by profit is built. 

CONFIGURATION OPTIONS 

The configuration options are read in a static constructor in ParkingTicketsStats, from a ParkingTicketsStats.properties
file that is under resources/c/kijiji/contest. 

The options are four:
streets.resource.filename=/centerline_clean.txt  
	the name of the resource file that holds the clean street names
byteBlocksize=20
	the size of the data chunk read by each spawned thread. Somewhere between 10 and 30 is the 
	sweet spot on my computer (2 years old Core i7). A value of 20 spawns 11 threads, and they
	seem to work fine
distance=local
	configures the algorithm to use the local similarity function. set to "dl" to use the
	Damerau-Levenshtein
threshold=0.7  
	minimum similarity value to consider the strings to be matching. 0.7 seems to work fine
	for both similarity functions
  
  
IMPLEMENTATION CAVEATS

Most of the details are in the comments, however a few things require special attention:

- Clean street names parsed from the additional dataset are stored in a 
	HashMap<String, Collections<String>>
  data structure. Keys are prefixes (3 letters) and values are all the clean street names
  that start with that prefix. I've tried several other options for this data structure 
  (tries, TreeMaps, additional internal data...) but a simple HashMap is the one that
  performs best. Values are really ArrayLists because they seem to be the ones with the
  best performance (strangely enough, I'd have expected hashmaps to perform better in this case..)
  
- Since each thread needs to access the clean street names, a copy of it is passed to the thread. 
  The very small copying overhead is widely compensated by the absence of access to shared objects
  in the heaviest part of the processing.
  
- I know that splitting the inputstream into fixed-size chunks leads to lines being split. The parser 
  thread will simply discard incomplete or malformed lines. On average, the number of broken lines 
  is the number of threads-1, which in my setup means 10 strings. I thought I wouldn't care much 
  about it, since you did not ask for perfection :-) However it could be fixed by having each thread
  store the first and last (broken) lines, then combining them back together and parsing. It 
  wouldn't have improved accuracy very much :-)
  
- Extracting street names and profits from the byte buffer is done with a custom ByteInputReader, 
  that works at the byte level and takes care of newlines, separators and so on. This, while being 
  a little ugly, beats the usual BufferedReader/readline/line.split/get_the_fields by a wide 
  score, because it will not build any string until it has a sort-of-clean street name, while the
  other approach builds tons of strings just to throw them away. And we know that this is very bad
  for the JVM :-) 
  The extracted street name is already cleaned from the number, some clutter and just the "ST" 
  suffix, because it leads to bad performance of the similarity functions (KING ST -> KINGSTON).
  Ugly and low-level, but very efficient. 
  
- Extracted streets are temporarily stored in a Hashmap<String, Street>, indexed by the parsed
  street name, without matching it to the clean street names, while profits are added.
  
- In a subsequent step - PTSRunner.pack() - the extracted streets are matched to the clean
  street names.
  Then data is accumulated in the shared "packedData" dictionary. Concurrence at this
  point is not a big issue, since any calculation or I/O operation has been completed, except for the
  trivial task of summing a street's profit to the same street's profit that is already in the
  shared object. Synchronization is applied to ensure consistence of the result. 
  
- The main thread waits for all parsers to complete the packing of data into the shared dictionary,
  then takes all the data and puts it in a StreetMap, which is the returned object.
  The sorting of keys according to profit is achieved by a using a simple Comparator: since all
  data is provided to the StreetMap at construction time, the Comparator itself can use the data
  to make the comparison, just making sure to return "0" based on string equality, and the difference
  of profits in all other cases. It is consistent with String.equals(), which is required by the 
  SortedMap contract, but sorts on profits :-). This approach mandates that the underlying data
  does not change, or at least that nobody changes a street's profit after the SortedMap has been created.
  
  To make things easier and avoid writing all the necessary code to keep the underlying aligned to
  the Map's content, I have set all read/write methods to throw an exception, i.e. the semantic is
  that this StreetMap is immutable in all due senses.     