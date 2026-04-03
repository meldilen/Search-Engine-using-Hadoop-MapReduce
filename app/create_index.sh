#!/bin/bash

INPUT_PATH="/input/data/part-*.csv"
TEMP_PATH="/tmp/term_doc_tf"
OUTPUT_PATH="/indexer"

echo "=== Pipeline 1: term, doc_id, tf and doclen markers ==="

hdfs dfs -rm -r -f $TEMP_PATH

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files "mapreduce/mapper1.py,mapreduce/reducer1.py" \
    -mapper "python3 mapper1.py" \
    -reducer "python3 reducer1.py" \
    -input $INPUT_PATH \
    -output $TEMP_PATH

echo "=== Pipeline 2: build index ==="

hdfs dfs -rm -r -f $OUTPUT_PATH/index

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files "mapreduce/mapper2.py,mapreduce/reducer2.py" \
    -mapper "python3 mapper2.py" \
    -reducer "python3 reducer2.py" \
    -input $TEMP_PATH \
    -output $OUTPUT_PATH/index

echo "=== Pipeline 3: document stats ==="

hdfs dfs -rm -r -f $OUTPUT_PATH/doc_stats

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files "mapreduce/mapper3.py,mapreduce/reducer3.py" \
    -mapper "python3 mapper3.py" \
    -reducer "python3 reducer3.py" \
    -input $TEMP_PATH \
    -output $OUTPUT_PATH/doc_stats

echo "=== Pipeline 4: total documents count ==="

hdfs dfs -rm -r -f $OUTPUT_PATH/N

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files "mapreduce/mapper4.py,mapreduce/reducer4.py" \
    -mapper "python3 mapper4.py" \
    -reducer "python3 reducer4.py" \
    -input $TEMP_PATH \
    -output $OUTPUT_PATH/N

echo "=== Cleaning up ==="
hdfs dfs -rm -r -f $TEMP_PATH

echo "=== Indexing completed ==="
hdfs dfs -ls $OUTPUT_PATH/
hdfs dfs -ls $OUTPUT_PATH/index/
hdfs dfs -ls $OUTPUT_PATH/doc_stats/
hdfs dfs -ls $OUTPUT_PATH/N/