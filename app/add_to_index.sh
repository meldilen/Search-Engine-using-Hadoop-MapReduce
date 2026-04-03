#!/bin/bash

source .venv/bin/activate

if [ -z "$1" ]; then
    echo "Usage: add_to_index.sh <path_to_local_txt_file>"
    echo "File name format: {id}_{title}.txt"
    echo "Example: add_to_index.sh ./123456_My_Document.txt"
    exit 1
fi

LOCAL_FILE="$1"
FILENAME=$(basename "$LOCAL_FILE")

echo "Adding new document: $FILENAME"

# check file name
if [[ ! $FILENAME =~ ^[0-9]+_.+\.txt$ ]]; then
    echo "ERROR: File name must be in format: {id}_{title}.txt"
    exit 1
fi

# copy file to container
docker cp "$LOCAL_FILE" cluster-master:/app/temp_doc.txt

docker exec cluster-master bash -c "
    source .venv/bin/activate
    hdfs dfs -put -f /app/temp_doc.txt /data/documents/$FILENAME
    spark-submit add_document.py /data/documents/$FILENAME
    rm /app/temp_doc.txt
"

echo "Document added successfully"

read -p "Press Enter to continue..."