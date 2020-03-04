#!/bin/bash
for year in 2007
do
    for month in $(seq 1 12)
    do
        echo "reddit year = $year, month = $month" 
        spark-submit sentimentAnalysis.py reddit append 0 $year $month
        echo "Finished reddit year = $year, month = $month" 
    done
done
