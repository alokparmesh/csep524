CSEP 524 Autumn 2016 - Parallel Computation (PMP) - Project 4

Part A: Parallel Matrix Multiply

Execution Instructions:
    In order to execute please run YOUR_SPARK_HOME/bin/spark-submit matrixVectorMultiply.py <inputfile.txt>
    It will write the output of multiplication into output.txt

Implementation:
    The implementation works by storing matrix in column indexed format for value and row.
    Next is multiplying each value with corresponding vector column value using join.
    It then reduces it to row indexed sums.
    Finally to output it sorts by row index, collects and stores result in output.txt


Part B: Parallel Breadth First Search
Execution Instructions:
    In order to execute please run YOUR_SPARK_HOME/bin/spark-submit bfs.py <inputfile.bin> <node to search>
    It will display the output of search on console.

Implementation:
    The implementation is to read edge input and reverse and union them to get all edge list as graph is undirected.
    Reduce edge list to vertex and list of child vetext tuple.
    Initialize a level rdd for each vertex with -1. Do bfs from root.
    Output to console the results of bfs.
