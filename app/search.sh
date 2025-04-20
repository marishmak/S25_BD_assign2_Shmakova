#!/bin/bash

# Check if query is provided
if [ -z "$1" ]; then
    echo "Usage: search.sh <query>"
    exit 1
fi

# Activate virtual environment
source .venv/bin/activate

# Run the query
spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.1.0 \
    --conf spark.cassandra.connection.host=cassandra-server \
    query.py "$1"


echo "Successfully search!"