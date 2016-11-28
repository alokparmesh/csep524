
Part A: Parallel Matrix Multiply

Execution Instructions:
    In order to execute please run make or make MatrixMultiply first.
    It will create MatrixMutliply.out as executable.
    Then run ./MatrixMultiply.out <inputfile.txt>
    It will write the output of multiplication into output.txt

Implementation:
    The implementation simply distributes the n rows of n x m matrix to x process where x = total process - 1.
    1 process (Rank 0) is reserved for input/output and coordination.


Part B: Parallel Breadth First Search
Execution Instructions:
    In order to execute please run make or make bfs first.
    It will create bfs.out as executable.
    Then run ./bfs.out <inputfile.bin> <node to search>
    It will display the output of search on console.

Implementation:
    The implementation is to affinitize a particalur node with particular process. All nodes are equidistributed to worker processes. 
    Coordinator process is takes care of input reading and providing each worker process with adjacency matrix. 
    It also coordinates bfs next level processing is required or not.
    End result is worker process returning back the distances of affinitized vertexes.
