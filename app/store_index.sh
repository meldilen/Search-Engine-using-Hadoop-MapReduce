#!/bin/bash

source .venv/bin/activate

echo "Waiting for Cassandra to be ready"
sleep 10

spark-submit \
    --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 \
    --conf spark.cassandra.connection.host=cassandra-server \
    --conf spark.cassandra.connection.port=9042 \
    store_index.py

echo "Store index completed"