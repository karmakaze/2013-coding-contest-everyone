This is Andrew Stevens attempt at the kijiji contest
https://github.com/kijiji-ca/2013-coding-contest

I used Java, with the google guava library and some threads via java.util.concurrent

Here are some solutions I found neat:
- andxyz  
https://github.com/andxyz/2013-coding-contest/tree/master/src/main/java/ca/kijiji/contest  
Short and sweet, not too many optimizations and a few magic numbers.

- devsoftweb  
https://github.com/devsoftweb/2013-coding-contest/tree/master/src/main/scala/ca/kijiji/contest  
Use of scala and parallel processing. Also it was short and sweet.

- kinghuang  
https://github.com/kinghuang/2013-coding-contest/blob/master/src/main/java/ca/kijiji/contest/ParkingTicketsStats.java  
Use of int cores = Runtime.getRuntime().availableProcessors(); was new to me. I just hardcoded 64 processors, though now that I read the contest rules, I should've just hardcoded 4 threads, for 4 processors.


