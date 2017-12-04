from __future__ import print_function

import sys
import re
from pyspark import SparkContext
from csv import reader
from csv import writer
from StringIO import StringIO
from datetime import datetime

null_indices = (0, 6, 8, 14, 7, 9, 19, 20, 21, 22, 23)
int_indices = (0, 6, 8, 14, 19, 20)
float_indices = (21, 22)
date_indexes = (1, 3, 5)
time_indexes = (2, 4)

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


def mark_null_in_date_and_time(line):
    all_indexes = date_indexes + time_indexes
    for i in all_indexes:
        if line[i] == '':
            line[i] = "null"


def mark_invalid_date(line):
    for i in date_indexes:
        date = line[i]
        if date == "null" or date in columns:
            pass
        else:
            year = int(datetime.strptime(line[i], '%m/%d/%Y').year)
            if year < 2006 or year > 2016 or date != datetime.strptime(date, "%m/%d/%Y").strftime("%m/%d/%Y"):
                line[i] = "invalid"
                return


def mark_invalid_time(line):
    for i in time_indexes:
        time = line[i]
        if time == "null" or time in columns:
            pass
        else:
            if line[i] == "24:00:00":
                line[i] = "00:00:00"
                time = line[i]
            if time != datetime.strptime(time, '%H:%M:%S').strftime('%H:%M:%S'):
                line[i] = "invalid"


def is_typeof_time(x):
    try:
        x = datetime.strptime(x, '%H:%M:%S')
    except ValueError:
        return False
    return True


def validate_crm_atpt_cptd_cd(row):
    status = ('COMPLETED', 'ATTEMPTED')
    data = row[10]
    if data in columns:
        return
    if data == "":
        row[10] = "null"
        return
    if type(data) != str or data not in status:
        row[10] = 'invalid'


def validate_law_cat_cd(row):
    offense_levels = ('FELONY', 'MISDEMEANOR', 'VIOLATION')
    data = row[11]
    if data in columns:
        return
    if data == "":
        row[11] = "null"
        return
    if type(data) != str or data not in offense_levels:
        row[11] = 'invalid'


def validate_boro_nm(row):
    boroughs = ('BROOKLYN', 'MANHATTAN', 'BRONX', 'QUEENS', 'STATEN ISLAND')
    data = row[13]
    if data in columns:
        return
    if data == "":
        row[13] = "null"
        return
    if type(data) != str or data not in boroughs:
        row[13] = 'invalid'


def validate_loc_of_occur_desc(row):
    premises = ('INSIDE', 'FRONT OF', 'OPPOSITE OF', 'REAR OF', 'OUTSIDE')
    data = row[15]
    if data in columns:
        return
    if data == "":
        row[13] = "null"
        return
    if type(data) != str or data not in premises:
        row[15] = 'invalid'


def validate_enum_attrs(row):
    validate_crm_atpt_cptd_cd(row)
    validate_law_cat_cd(row)
    validate_boro_nm(row)
    validate_loc_of_occur_desc(row)
    return row


def mark_null(line):
    for i in null_indices:
        if line[i] in columns:
            pass
        if line[i] == '':
            line[i] = "null"


def mark_invalid_int(line):
    for i in int_indices:
        if line[i] in columns or line[i] == "null":
            pass
        else:
            reg = "^[-]?\d+$"
            if re.match(reg, line[i]) is None:
                line[i] = "invalid"


def mark_invalid_float(line):
    for i in float_indices:
        if line[i] in columns or line[i] == "null":
            pass
        else:
            reg = "^[-]?\d+?\.\d+?$"
            if re.match(reg, line[i]) is None:
                line[i] = "invalid"


def mark_invalid_coord(line):
    if line[23] == "null" or line[23] in columns:
        return line
    reg = "^(\([-+]?\d{1,2}[.]\d+),\s*([-+]?\d{1,3}[.]\d+\))$"
    res = re.match(reg, line[23])
    if res is None:
        line[23] = "invalid"
    return line


def mark_invalid(line):
    mark_null(line)
    mark_invalid_int(line)
    mark_invalid_float(line)
    mark_invalid_coord(line)
    mark_null_in_date_and_time(line)
    mark_invalid_date(line)
    mark_invalid_time(line)
    return validate_enum_attrs(line)


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

    lines = lines.mapPartitions(lambda line: reader(line)) \
        .map(lambda line: mark_invalid(line)) \
        .map(lambda line: repack(line)) \
        .saveAsTextFile("marked_output")
    sc.stop()
