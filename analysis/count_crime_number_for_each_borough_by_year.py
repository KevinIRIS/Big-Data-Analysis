from __future__ import print_function
from pyspark import SQLContext
from pyspark import SparkContext
from datetime import datetime
import sys
import os
import tempfile
from pyspark.sql.functions import lit,year,col,column,count,when
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: bigram <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    dataframe = sqlContext.load(source="com.databricks.spark.csv", path=sys.argv[1], header=True, inferSchema=True)\
        .withColumn("year", column("RPT_DT").substr(7,4))
    boroughs = ('BROOKLYN', 'MANHATTAN', 'BRONX', 'QUEENS', 'STATEN ISLAND')
    res = dataframe.select("year").withColumnRenamed("year","year_data").distinct()
    for borough in boroughs:
        df = dataframe.filter(column("BORO_NM") == borough).groupBy("year").count()
        res = df.join(res, df["year"] == res["year_data"]).drop(df.year)
    res.save('count_crime_number_for_each_borough_by_year.csv', 'com.databricks.spark.csv')
    sc.stop()