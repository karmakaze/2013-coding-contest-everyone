Repository Notes
================
- This 'master' branch has only updated this README.md file.

- The 'submit' branch is what I actually submitted. It uses a large hash table with a 'perfect has' function (but not a minimal perfect hash) the table in implemented with String[] for the keys and AtomicIntegerArray for the values.

- The 'closed-atomic' branch is what I wanted to finish but didn't get done before the deadline. It runs ~475 ms on my Linux i7-2600 3.4GHz machine. It uses 8 threads (HTT) and tries to keep all 'working' data and the closed hash table within 8 MB of L3 shared cache memory. Instead of comparing entire keys, 32-bit hash values are compared using optimistic local reads and falls back using a memory barrier. 
The values are in an AtomicIntegerArray which allows concurrent updates of different elements.

Cheers,
-Keith
