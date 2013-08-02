This is Andrew Stevens attempt at the kijiji contest
https://github.com/kijiji-ca/2013-coding-contest

I used Java, with the google guava library and some threads via java.util.concurrent

Here are some solutions I found neat:
- andxyz (me, some guy from Toronto)  
    - https://github.com/andxyz/2013-coding-contest/tree/master/src/main/java/ca/kijiji/contest  
    - https://twitter.com/andrewstevens
    - http://andxyz.com
    - Short and sweet, not too many optimizations and a few magic numbers.

- devsoftweb (Man from montreal, impressing me with his Scala skill)
    - https://github.com/devsoftweb/2013-coding-contest/tree/master/src/main/scala/ca/kijiji/contest  
    - https://twitter.com/devsoftweb http://blog.devsoftweb.com/about
    - Use of scala and parallel processing.(Really cool to see)
    - It was short and sweet like mine (not a lot of classes)

- kinghuang  (He's from Calgary I think)
    - https://github.com/kinghuang/2013-coding-contest/blob/master/src/main/java/ca/kijiji/contest
    - https://twitter.com/kinghuang  http://wiredflux.net/
    - Use of int cores = Runtime.getRuntime().availableProcessors(); was new to me. I just hardcoded 64 processors (hoping they had a beefy machine), though now that I read the contest rules, I should've just hardcoded 4ish threads. (max is 4 processors in contest rules).
    - Seems obsessed with speed, which is cool.
    - Asked some questions in the comment thread of http://kijijiblog.ca/so-you-think-you-can-code-eh/

- acbellini (She's from italy I think? How cool is that)
    - https://github.com/acbellini/2013-coding-contest/tree/master/src/main/java/ca/kijiji/contest
    - http://www.webeggs.net/ 
    - Heavyweight style of threading. Used a neat String comparison algorithm. Lots of extra classes to browse around.

- JordanMilne (Seems to like python and low level processing, be it captchas, images or odd filesystem compressions)
    - https://github.com/JordanMilne/2013-coding-contest/blob/master/src/main/java/ca/kijiji/contest
    - Some really interesting thoughts on threading for performace. (considering how little thought I gave it, I find all threading talk interesting now, haha)
        - // Use as many workers as we have free cores, up to a maximum of 3 workers, always using at least one.
        - // A single worker thread is fastest when the main thread is wasting its time skipping tickets anyways.
        
        