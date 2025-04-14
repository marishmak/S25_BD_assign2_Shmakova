#!/bin/bash

# Activate virtual environment
source .venv/bin/activate

# Python of the driver (/app/.venv/bin/python)
export PYSPARK_DRIVER_PYTHON=$(which python)

unset PYSPARK_PYTHON

# Check if parquet file exists locally
if [ ! -f "/data/a.parquet" ]; then
    echo "Error: Parquet file '/data/a.parquet' not found."
    exit 1
fi

# Create HDFS directory and copy parquet file
echo "Copying parquet file to HDFS..."
hdfs dfs -mkdir -p /index/data || { echo "Error: Failed to create HDFS directory."; exit 1; }
hdfs dfs -put /data/a.parquet /index/data/a.parquet || { echo "Error: Failed to copy parquet file to HDFS."; exit 1; }

# Verify the file exists in HDFS
echo "Verifying HDFS file..."
hdfs dfs -ls /index/data/a.parquet || { echo "Error: File not found in HDFS."; exit 1; }

# Generate data using PySpark
echo "Generating data..."
spark-submit prepare_data.py || { echo "Error: Spark job failed."; exit 1; }

# Verify output
echo "Verifying output files..."
ls /app/data || { echo "Error: Output files not found."; exit 1; }

echo "Data preparation completed successfully!"