from __future__ import print_function
from pyspark import SQLContext
from pyspark import SparkContext
from datetime import datetime
import sys
import os
import tempfile
from pyspark.sql.functions import *
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: bigram <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    dataframe = sqlContext.load(source="com.databricks.spark.csv", path=sys.argv[1], header=True, inferSchema=True)
    dataframe = dataframe.withColumn("year", column("RPT_DT").substr(7,4)).withColumn("month", column("RPT_DT").substr(0,2))
    df = dataframe.groupBy("RPT_DT").count()
    df.save('count_crime_by_day.csv','com.databricks.spark.csv')
    sc.stop()