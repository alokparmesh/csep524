CSEP 524 Autumn 2016 - Parallel Computation (PMP) - Project 1

Part A: Parallel Matrix Multiply

Execution Instructions:
    In order to execute please run make or make MatrixMultiply first.
    It will create MatrixMutliply.out as executable.
    Then run ./MatrixMultiply.out <inputfile.txt>
    It will write the output of multiplication into output.txt

Implementation:
    The implementation simply distributes the n rows of n x m matrix to x number of threads.
    (number of threads = no. of processors) 


Part B: Parallel Breadth First Search
Execution Instructions:
    In order to execute please run make or make bfs first.
    It will create bfs.out as executable.
    Then run ./bfs.out <inputfile.bin> <node to search>
    It will display the output of search on console.

Implementation:
    The implementation is to create a shared queue for each level of the graph given a starting root node. There is an associated lock with it. 
    It then creates worker threads which remove a node from current level queue and insert the children which are unvisited to next level’s queue. 
    They acquire release lock while accessing shared resources.
    (number of threads = no. of processors)
