from __future__ import print_function
from pyspark import SQLContext
from pyspark import SparkContext
import sys
import os
import tempfile
from pyspark.sql.functions import column,count,when
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: bigram <file>", file=sys.stderr)
	exit(-1)
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    dataset = sqlContext.load(source="com.databricks.spark.csv", path=sys.argv[1], header=True, inferSchema=True)
    columns = ('CMPLNT_NUM',
           'CMPLNT_FR_DT',
           'CMPLNT_FR_TM',
           'CMPLNT_TO_DT',
           'CMPLNT_TO_TM',
           'RPT_DT',
           'KY_CD',
           'OFNS_DESC',
           'PD_CD',
           'PD_DESC',
           'CRM_ATPT_CPTD_CD',
           'LAW_CAT_CD',
           'JURIS_DESC',
           'BORO_NM',
           'ADDR_PCT_CD',
           'LOC_OF_OCCUR_DESC',
           'PREM_TYP_DESC',
           'PARKS_NM',
           'HADEVELOPT',
           'X_COORD_CD',
           'Y_COORD_CD',
           'Latitude',
           'Longitude',
           'Lat_Lon'
           )
    def repack(list):
	res = {}
	for i in range(len(list)):
	    res[columns[i]] = list[i]
	return res
    dataset.select([count(when(column(index) == "null", index)).alias(index) for index in dataset.columns]).rdd.map(lambda x: repack(x)).coalesce(1).saveAsTextFile("count_null_col.csv")
    sc.stop()

