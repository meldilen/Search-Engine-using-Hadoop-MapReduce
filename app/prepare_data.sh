#!/bin/bash
# export LANG=en_US.UTF-8
# export LC_ALL=en_US.UTF-8

source .venv/bin/activate


# Python of the driver (/app/.venv/bin/python)
export PYSPARK_DRIVER_PYTHON=$(which python) 


unset PYSPARK_PYTHON

sleep 5

hdfs dfs -mkdir -p /data
hdfs dfs -mkdir -p /input

hdfs dfs -put -f data/d.parquet /data/

spark-submit --master local --driver-memory 2g prepare_data.py

echo "Copying text documents to HDFS..."
hdfs dfs -put -f data /data/documents

echo "HDFS /data"
hdfs dfs -ls /data/

echo "HDFS /input/data"
hdfs dfs -ls /input/data/

rm -f data/*.txt