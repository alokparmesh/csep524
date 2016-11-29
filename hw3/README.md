CSEP 524 Autumn 2016 - Parallel Computation (PMP) - Project 3

Part A: Parallel Matrix Multiply

Execution Instructions:
    In order to execute please run make or make MatrixMultiply first.
    It will create MatrixMutliply.out as executable.
    Then run mpirun -np <number of process>./MatrixMultiply.out <inputfile.txt>
    It will write the output of multiplication into output.txt

Implementation:
    The implementation assigns 1 process (Rank 0) is reserved for input/output and coordination and remaining processes as workers.
    Coordinator broadcasts common information and iterates each row and assignes it in a round robin manner among workers.
    It collects the output from each worker for each row and stores it. 


Part B: Parallel Breadth First Search
Execution Instructions:
    In order to execute please run make or make bfs first.
    It will create bfs.out as executable.
    Then run mpirun -np <number of process> ./bfs.out <inputfile.bin> <node to search>
    It will display the output of search on console.

Implementation:
    The implementation is to have one process do the coordination and rest of them do the graph processing.
    Coordinator process reads input and broadcasts the graph structure to workers.
    We affinitize a particalur node with particular process. All nodes are equidistributed among worker processes. 
    Each worker maintains its own current level queue and one next level queue for each worker process including iteself.
    Coordinator process merges the next level queue for each worker process and determines if there needs to be an exit or not based on woker queue size.
    In the end each worker process returning back the distances of affinitized vertexes.
