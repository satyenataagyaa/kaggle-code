#!/bin/bash

echo "date,high,low,open,close,volume,adj_close,Name" > all_stocks_5yr.csv
cd individual_stocks_5yr
files=$(ls *.csv)
for file in $files
do
	tail -n +2 $file >> ../all_stocks_5yr.csv
done
