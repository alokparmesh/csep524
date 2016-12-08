from __future__ import print_function

import struct
import sys
from pyspark import SparkContext

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: matrixVectorMultiply <inputfile> <root>", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="bfs.py")
    bFile = sc.binaryRecords(sys.argv[1], 16)
    root = sys.argv[2]

    # Read all edge inputs, reverse edges and union as graph is undirected
    edgeInputs = bFile.map(lambda x: struct.unpack("<qq",x))
    allEdgeList = sc.union([edgeInputs,edgeInputs.map(lambda x:(x[1],x[0]))])
    totalEdgeCount = allEdgeList.count()

    # Reduce edge list to tuple of vertex and array of child vertices
    inputGraph = allEdgeList.map(lambda edge:(edge[0],[edge[1]])).reduceByKey(lambda a,b:a+b)
    totalVertexCount = inputGraph.count()

    distances = inputGraph.map(lambda x : (x[0],-1))

    reached_vertices = distances.filter(lambda x: x[1] != -1).count()
    max_level = distances.reduce(lambda a,b: (-1, max(a[1],b[1])))[1]

    print("Graph vertices: {} with total edges {}.  Reached vertices from {} is {} and max level is {}\n".format(totalVertexCount, totalEdgeCount, root, reached_vertices, max_level))   

    sc.stop()
