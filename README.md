Repository Notes
================
- This 'master' branch has only updated this README.md file.

- The 'submit' branch is what I actually submitted. It uses a large hash table with a 'perfect hash' function (but not a minimal perfect hash). The table is implemented with a String[] for the keys using lock-free writes and an AtomicIntegerArray for the values.

- The 'closed-atomic' branch is what I wanted to finish but didn't get done before the deadline. It runs ~475 ms on my Linux i7-2600 3.4GHz machine. It uses 8 threads (HTT) and tries to keep all 'working' data and the closed hash table within 8 MB of L3 shared cache memory. Instead of comparing entire keys, 32-bit hash values are compared using optimistic lock-free reads and a memory barrier for re-reads/writes. The values are in an AtomicIntegerArray.

Cheers,
-Keith
