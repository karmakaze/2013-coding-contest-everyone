This is Andrew Stevens attempt at the kijiji contest
https://github.com/kijiji-ca/2013-coding-contest

I used Java, with the google guava library and some threads via java.util.concurrent

Here are some solutions I found neat:
- andxyz (me, some guy from Toronto)  
    - https://github.com/andxyz/2013-coding-contest/tree/master/src/main/java/ca/kijiji/contest  
    - https://twitter.com/andrewstevens
    - http://andxyz.com
    - Use of Java, short and sweet, not too many optimizations and a few magic numbers.

- devsoftweb (Man from montreal, impressing me with his Scala skill)
    - https://github.com/devsoftweb/2013-coding-contest/tree/master/src/main/scala/ca/kijiji/contest  
    - https://twitter.com/devsoftweb http://blog.devsoftweb.com/about
    - Use of Scala and parallel processing.(Really cool to see)
    - It was short and sweet like mine (not a lot of classes)

- kinghuang  (He's from Calgary I think)
    - https://github.com/kinghuang/2013-coding-contest/blob/master/src/main/java/ca/kijiji/contest
    - https://twitter.com/kinghuang  http://wiredflux.net/
    - Use of int cores = Runtime.getRuntime().availableProcessors(); was new to me. I just hardcoded 64 processors (hoping they had a beefy machine), though now that I read the contest rules, I should've just hardcoded 4ish threads. (max is 4 processors in contest rules).
    - Use of Java, Seems obsessed with speed, which is cool.
    - Asked some questions in the comment thread of http://kijijiblog.ca/so-you-think-you-can-code-eh/

- karmakaze (Keith Kim, Canada)
    - http://keithkim.ca
    - Speed crazy https://github.com/karmakaze/2013-coding-contest#readme

- whoward (William Howard, Toronto, seems to like he knows several languages and likes: processing data, 3d things, research)
    - https://github.com/whoward/2013-coding-contest/tree/master/src/main/java/ca/kijiji/contest
    - https://github.com/whoward/2013-coding-contest/tree/master/src/jruby
    - https://sites.google.com/site/whowardtke/Home
    - Use of JRuby, really interesting read being ruby on the jvm

- ftufek
    - https://github.com/ftufek/2013-coding-contest/blob/master/src/main/java/ca/kijiji/contest
    - Use of xtend. Didn't even know xtend was a language... huh.

- acbellini (She's from italy I think? How cool is that)
    - https://github.com/acbellini/2013-coding-contest/tree/master/src/main/java/ca/kijiji/contest
    - http://www.webeggs.net/ 
    - Use of Java, Heavyweight style of threading. Used a neat String comparison algorithm. Lots of extra classes to browse around.

- JordanMilne (Seems to like python and low level processing, be it captchas, images or odd filesystem compressions)
    - https://github.com/JordanMilne/2013-coding-contest/blob/master/src/main/java/ca/kijiji/contest
    - Use of Java, Some really interesting thoughts on threading for performance. (considering how little thought I gave it, I find all threading talk interesting now, haha)
        - // Use as many workers as we have free cores, up to a maximum of 3 workers, always using at least one.
        - // A single worker thread is fastest when the main thread is wasting its time skipping tickets anyways.

- epwatson (Eamonn Watson from Kitchener)
    - https://github.com/epwatson/2013-coding-contest/tree/master/src/main/java/ca/kijiji/contest
    - http://blog.eamonnwatson.com/
    - Use of Java, short and sweet, reminds me of mine.

- burnison (Richard Burnison, Canada)
    - https://github.com/burnison/2013-coding-contest/blob/master/src/main/java/ca/kijiji/contest/ParkingTicketsStats.java
    - http://burnison.ca/
    - Use of Java, I didn't follow it all that well

- soulofpeace (Choon Kee Oh, Singapore)
    - http://soulofpeace.tumblr.com/
    - https://github.com/soulofpeace/2013-coding-contest/blob/master/src/main/scala/ca/kijiji/contest/ParkingTicketsStatsApp.scala
    - Scala

- hrendalf (Unknown, seems like he just joined github to share his solution for this, cool!)
    - https://github.com/hrendalf/2013-coding-contest/blob/master/src/main/java/ca/kijiji/contest/ParkingTicketsStats.java
    - https://github.com/hrendalf/2013-coding-contest (explains it well enough)

- jdahan80 (Jonathan Dahan, Ottawa, Canada)
    - https://github.com/jdahan80/2013-coding-contest/blob/master/src/main/java/ca/kijiji/contest/ParkingTicketsStats.java
    - Dead simple parsing, I like it.
