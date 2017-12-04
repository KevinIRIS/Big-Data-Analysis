from __future__ import print_function

import sys
import re
from pyspark import SparkContext
from csv import reader
from csv import writer
from StringIO import StringIO

#filter if there is no id key code
#filter if offense description is ''  or key code is ''
#filter if start,end date and time are ''
#filter if borough or precint is ''
null_indices = (0,6,8,14,7,9,19,20,21,22,23)
int_indices = (0,6,8,14,19,20)
float_indices = (21,22)
def mark_null(line):
    for i in null_indices:
        if line[i] == '':
            line[i] = "null"
    return line

def mark_invalid_int(line):
    for i in int_indices:
        if line[i] == "null":
            pass
        else:
            reg = "^[-]?\d+$"
            if re.match(reg, line[i]) is None:
                line[i] = "invalid"
    return line

def mark_invalid_float(line):
    for i in float_indices:
        if line[i] == "null":
            pass
        else:
            reg = "^[-]?\d+?\.\d+?$"
            if re.match(reg, line[i]) is None:
                line[i] = "invalid"
    return line

def mark_invalid_coord(line):
    if line[23] == "null":
        return line
    reg = "^(\([-+]?\d{1,2}[.]\d+),\s*([-+]?\d{1,3}[.]\d+\))$"
    res = re.match(reg, line[23])
    if res is None:
        line[23] = "invalid"
    return line

def mark_invalid(line):
    line = mark_null(line)
    line = mark_invalid_int(line)
    line = mark_invalid_float(line)
    return mark_invalid_coord(line)


# def filter_file(line):
#     for index in indices:
#         if line[index] == '':
#             return False
#     return True
#     #return (line[1] == '' and  line[2] == '') or (line[3] == '' and line[4] == '')

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
                .map(lambda line: mark_invalid(line))\
                .map(lambda line: repack(line))\
                .saveAsTextFile("filter_lines")
    sc.stop()
