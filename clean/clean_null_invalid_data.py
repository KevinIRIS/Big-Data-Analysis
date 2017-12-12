from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from csv import writer
from StringIO import StringIO
#filter if there is no id key code
#filter if offense description is ''  or key code is ''
#filter if start,end date and time are ''
#filter if borough or precint is ''
indices = (0,6,7,14)
def filter_file(line):
    for index in indices:
        if line[index] == 'null' or line[index] == "invalid":
            return False
    return True

def repack(line):
    res = StringIO("")
    writer(res).writerow(line)
    return res.getvalue().strip()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: bigram <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)

    lines = lines.mapPartitions(lambda line: reader(line))\
                .filter(lambda line: filter_file(line))\
                .map(lambda line: repack(line))
    lines.saveAsTextFile("filtered_output")
    sc.stop()
