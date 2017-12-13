#!/usr/bin/env bash

wget https://data.cityofnewyork.us/api/views/qgea-i56i/rows.csv?accessType=DOWNLOAD

mv rows.csv?accessType=DOWNLOAD crimes.csv

wget https://data.cityofnewyork.us/api/views/37cg-gxjd/rows.csv?accessType=DOWNLOAD

mv rows.csv?accessType=DOWNLOAD population.csv

hfs -put crimes.csv
hfs -put population.csv
hfs -put income.csv