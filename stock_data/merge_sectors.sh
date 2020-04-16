#!/bin/bash

echo "date,high,low,open,close,volume,adj_close,Name" > all_sectors_5yr.csv
cd individual_sectors_5yr
files=$(ls *.csv)
for file in $files
do
	tail -n +2 $file >> ../all_sectors_5yr.csv
done
