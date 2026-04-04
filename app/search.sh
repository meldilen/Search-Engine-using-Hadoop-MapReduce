#!/bin/bash

source .venv/bin/activate

# Python of the driver (/app/.venv/bin/python)
export PYSPARK_DRIVER_PYTHON=$(which python) 

# Python of the excutor (./.venv/bin/python)
export PYSPARK_PYTHON=./.venv/bin/python

if [ -z "$1" ]; then
    echo "Usage: search.sh <query>"
    echo "Example: search.sh \"dogs cats\""
    exit 1
fi

QUERY="$1"

echo "Running BM25 search"
echo "Query: $QUERY"

spark-submit \
    --master yarn \
    --deploy-mode client \
    --archives /app/.venv.tar.gz#.venv \
    --conf spark.cassandra.connection.host=cassandra-server \
    --conf spark.cassandra.connection.port=9042 \
    query.py "$QUERY"

echo "Search completed"