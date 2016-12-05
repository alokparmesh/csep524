from __future__ import print_function

import sys
from operator import add

from pyspark import SparkContext

def splitWith(rowString, rowIndex):
    colValue = list(map(int, rowString.split(',')))
    retValue = []

    for colIndex,item in enumerate(colValue):
        retValue.append((colIndex,(rowIndex,item)))

    return retValue

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: matrixVectorMultiply <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="matrixVectorMultiply")
    inputFile = sc.textFile(sys.argv[1], 1)
    rowCount,columnCount = map(int,inputFile.first().split(','))
    inputFileWithIndex = inputFile.zipWithIndex()
    matrixInput = inputFileWithIndex.filter(lambda x : x[1] > 0 and x[1] <= rowCount)
    vectorInput = inputFileWithIndex.filter(lambda x : x[1] > rowCount)
    colIndexedVector = vectorInput.map(lambda x : (x[1] - rowCount -1,int(x[0])))
    colIndexedMatrix = matrixInput.flatMap(lambda x: splitWith(x[0],x[1]-1))
    rowIndexedSum = colIndexedMatrix.join(colIndexedVector).map(lambda x: (x[1][0][0], x[1][0][1] * x[1][1])).reduceByKey(add)
    output = rowIndexedSum.sortByKey().map(lambda x: x[1]).collect()
    with open('output.txt', 'w') as outputFile:
        for item in output:
            outputFile.write("{0}\n".format(item))

    sc.stop()
