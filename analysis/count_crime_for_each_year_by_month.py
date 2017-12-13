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
    res = dataframe.select("month").withColumnRenamed("month", "month_data").distinct()
    for year in range(2006,2017):
        df = dataframe.filter(column("year") == str(year)).groupBy("month").count()
        res = df.join(res, df["month"] == res["month_data"]).drop(df.month)
    res.save('count_crime_for_each_year_compare_by_month.csv', 'com.databricks.spark.csv')
    sc.stop()