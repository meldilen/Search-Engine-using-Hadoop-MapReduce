#!/bin/bash
# Start ssh server
service ssh restart 

# Starting the services
bash start-services.sh

# Wait for services to be ready
echo "Waiting for Hadoop and Cassandra to be ready..."
sleep 30

# Creating a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install any packages
pip install -r requirements.txt  

# Package the virtual env.
venv-pack -o .venv.tar.gz

# Collect data
bash prepare_data.sh


# Run the indexer
bash index.sh

# Run the ranker
# bash search.sh "this is a query!"

echo "Query 1: 'football player'"
echo "football player" | spark-submit --master local query.py

echo "Query 2: 'Russian music artist'"
echo "Russian music artist" | spark-submit --master local query.py

echo "Query 3: 'history of Belarus'"
echo "history of Belarus" | spark-submit --master local query.py

echo "Container is running. You can run custom searches:"
echo "  docker exec -it cluster-master bash"
echo "  ./search.sh \"your query\""

# Keep container alive
tail -f /dev/null
