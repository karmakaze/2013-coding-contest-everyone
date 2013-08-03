I usually use C++ for high-performance code, so this contest was actually a good excuse to explore high-performance concurrent programming in Java. I hope the comments are clear since my code is pretty much an executable blog post.

Overview
========

My solution has 5 main parts: Coordination, I/O, the ticket workers, the street resolver, and map finalization.

Coordination and I/O
====================

We use up to 3 worker threads for this solution, always leaving a core free for the main thread to keep reading since the solution's mostly I/O bound.

The workers all share a queue to pull new rows from, a street resolver to get street names from an address, and a ConcurrentHashMap of street -> profit (more on those later.)

Nothing too special here for the I/O, just a BufferedReader and readLine(). 
We read the header line, split it into columns and give it to the workers so we can convert from column names to column indexes quickly. 
From then on the results of readLine() get put on the work queue. After we run out of rows to put on the queue, we block until the workers to finish up.

Ticket Workers
==============

Each worker runs in its own thread, it pulls rows off of the shared work queue until it receives the signal to stop. 
The worker will try to lazily split the row into columns by splitting on columns. It checks if this returned the expected number of columns (it won't if the row contains escaped commas,)
and uses a proper method of parsing the row that takes escaping into account if necessary.

The row's columns then get passed to the ticket workers processTicketCols method (StreetProfitTabulator's in this case.)

StreetProfitTabulator gets the location2 column, trims it, and resolves the street name from the address. It then looks up the street's entry in the stats map, gets its AtomicInteger and adds the ticket's fine to it.

I went with a ConcurrentHashMap containing AtomicIntegers that all the workers shared for a few reasons. Each worker having its own profit map to be merged at the end 
seemed a bit cumbersome, and any other concurrent solution would have required tons of locks. No point locking when you don't need to, with this solution you only need when adding a new street to the map.
get() is lock-free and you can safely add to the total profit with the AtomicInteger you get from get(). As far as I can tell, it's also at least as fast as separate maps, probably faster. I also have an irrational hatred for locks.

The Street Name Resolver
========================

This is the real meat of the solution. A reference to the street name resolver is given to all of the workers, so they may share a cache of address -> street names.
We try to take the street number off the address using cheap string operations, then check the map to see if this numberless address already has a street name associated with it.

If it does, we're done! If not, we need to pull the street out of the address and ignore the surrounding gunk using regular expressions.

Then we pass the street (with or without a designation and a direction) to a function that removes all directions and the street designation from the end of the string and returns the street name.
The result is then stored in the shared address -> street name cache so that addresses like this one can be looked up quickly. 

If at any time it's determined that the address isn't valid, null is returned, otherwise the address' street name is returned.

All of this is an oversimplified summary, there are tons of comments in StreetNameResolver.java that explain it better.

Map Finalization
================

This is probably the jankiest part of the code, and I definitely could have done this better. If I had to do it again, I would have found some way to use Ordering.natural().reverse() instead.

Looks like this actually fills the requirements for compare != 0 for streets with the same value.

Since we use a map of AtomicIntegers internally, we need to make an immutable sorted map of Integers. We make a list of the street names sorted by the value of their associated AtomicInteger, and use that list as the order for the immutable map.

We then add all of the keys and values (as Integers) to the immutable map and return it.


Other Optimizations
===================

This one isn't used by default in my solution since it introduces inaccuracies into the data,  andall of the requirements for the implementation
weren't known at the start of the contest, but there's a second method signature for sortStreetsByProfitability.
It allows you to specify a number of tickets to skip by for each one added to the work queue. To compensate for the skipped tickets, a streets profit is multiplied by skipped + 1.
It's even almost kind of [a valid sampling method](https://en.wikipedia.org/wiki/Sampling_%28statistics%29#Systematic_sampling)!
With a skip of 6, the algorithm continues to satisfy the test case while taking 40% as long as processing the whole file.

Stuff I Nixed
=============

My own CSV parser, since it would only get run a handful of times (when splitting on commas failed) and I wanted to spare people my C++-isms.

A BufferedReader implementation with a skipLine() method, it skip()ped the number of characters in the shortest line in the file and then calls readLine() to eat the rest of the line.
I dropped it because it would have tied the reader to this particular file (by relying on the length of the file's shortest line) and it would only be used when numSkipLines != 0 anyways.
The contest isn't to re-implement half of the stdlib anyways.

Pretty much anything where speed came at the cost of readability or flexibility.

TL;DR
=====

I think I can program, I don't think I can write documentation.
