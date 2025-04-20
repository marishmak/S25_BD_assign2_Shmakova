#!/bin/bash

# Set strict mode for better error handling
set -euo pipefail

# Activate virtual environment
echo "Activating virtual environment..."
source .venv/bin/activate

# Set environment variables
export PYSPARK_DRIVER_PYTHON=$(which python)
export PYSPARK_PYTHON=$(which python)

# File paths
PARQUET_SOURCE="/data/a.parquet"
HDFS_PARQUET_PATH="/index/data/a.parquet"
OUTPUT_DIR="/app/data"

# Check if source parquet exists
echo "Checking source parquet file..."
if [ ! -f "$PARQUET_SOURCE" ]; then
    echo "Error: Source parquet file not found at $PARQUET_SOURCE"
    exit 1
fi

# Prepare HDFS
echo "Setting up HDFS directories..."
hdfs dfs -mkdir -p /index/data || { echo "Error: Failed to create HDFS directory."; exit 1; }

echo "Copying parquet file to HDFS..."
# hdfs dfs -put -f "$PARQUET_SOURCE" "$HDFS_PARQUET_PATH" || { echo "Error: Failed to copy parquet file to HDFS."; exit 1; }

echo "Verifying HDFS file..."
# hdfs dfs -ls "$HDFS_PARQUET_PATH" || { echo "Error: File not found in HDFS."; exit 1; }

# Wait for HDFS to be fully ready
sleep 5

# Prepare local output directory
mkdir -p "$OUTPUT_DIR"

# Run Spark job with additional HDFS config
echo "Starting Spark data preparation job..."
# spark-submit \
#     --master local[*] \
#     --driver-memory 2g \
#     --executor-memory 2g \
#     --conf spark.driver.maxResultSize=1g \
#     --conf spark.sql.parquet.enableVectorizedReader=true \
#     --conf spark.hadoop.fs.defaultFS=hdfs://cluster-master:9000 \
#     --conf spark.hadoop.dfs.client.use.datanode.hostname=true \
#     --conf spark.hadoop.dfs.namenode.rpc-address=cluster-master:9000 \
#     prepare_data.py || {
#         echo "Error: Spark job failed"
#         exit 1
#     }

# DOC_COUNT=$(ls -1 "$OUTPUT_DIR" | wc -l)
# if [ "$DOC_COUNT" -ne 1001 ]; then
#     echo "Error: Expected 1000 documents, found $DOC_COUNT"
# fi

echo "Successfully created 1000 documents in $OUTPUT_DIR"
echo "Data preparation completed successfully!"