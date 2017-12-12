hfs -put crime.csv

pyspark mark_null_invalid_columns.py crime.csv

hfs -getmerge marked_ouput marked_output

hfs -put marked_ouput

hfs clean_null_invalid_data.py marked_output
