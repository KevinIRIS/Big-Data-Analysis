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
    df = dataframe.groupBy("OFNS_DESC").count().sort("count",ascending = False).limit(5)
    df = df.withColumn("Percentage",df["count"]/5560757)
    df.save('count_top_five_crime_number.csv','com.databricks.spark.csv')
    sc.stop()