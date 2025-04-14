#!/bin/bash
echo "This script will include commands to search for documents given the query using Spark RDD"

# Activate the virtual environment
source .venv/bin/activate

# Python of the driver (/app/.venv/bin/python)
export PYSPARK_DRIVER_PYTHON=$(which python) 

# Python of the executor (./.venv/bin/python)
export PYSPARK_PYTHON=./.venv/bin/python

# Check if a query is provided
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 'your query here'"
    exit 1
fi

QUERY=$1

# Submit the PySpark application to YARN
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --archives /app/.venv.tar.gz#.venv \
    query.py <<< "$QUERY"
