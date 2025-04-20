#!/bin/bash

# Set default input path
INPUT_PATH=${1:-"/index/data"}

# Absolute path to mapper and reducer files
MAPPER_PATH="/app/mapreduce/mapper1.py"
REDUCER_PATH="/app/mapreduce/reducer1.py"

# Check if mapper and reducer files exist
if [ ! -f "$MAPPER_PATH" ]; then
    echo "Error: Mapper file not found at $MAPPER_PATH"
    exit 1
fi

if [ ! -f "$REDUCER_PATH" ]; then
    echo "Error: Reducer file not found at $REDUCER_PATH"
    exit 1
fi

# Temporary HDFS path
TMP_HDFS_PATH="/index/tmp/input"

# Clean up any existing temp files
hadoop fs -rm -r -f "$TMP_HDFS_PATH" 2>/dev/null
hadoop fs -rm -r -f /index/output 2>/dev/null

# Check if input is local file/dir or HDFS path
if [[ $INPUT_PATH == /* ]]; then
    # Local file/dir - copy to HDFS
    echo "Copying local input to HDFS..."
    hadoop fs -mkdir -p /index/tmp
    hadoop fs -put "$INPUT_PATH" "$TMP_HDFS_PATH" || {
        echo "Error: Failed to copy input to HDFS"
        exit 1
    }
    INPUT_HDFS="$TMP_HDFS_PATH"
else
    # HDFS path
    INPUT_HDFS="$INPUT_PATH"
    # Verify HDFS path exists
    hadoop fs -test -e "$INPUT_HDFS" || {
        echo "Error: Input path $INPUT_HDFS not found in HDFS"
        exit 1
    }
fi

echo "Indexing files from: $INPUT_HDFS"

# Package the virtual environment
if [ -f "/app/.venv.tar.gz" ]; then
    rm -f /app/.venv.tar.gz
fi
(cd /app && venv-pack -o /app/.venv.tar.gz)

# Run the MapReduce job with proper resource allocation
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -D mapreduce.job.reduces=4 \
    -D mapreduce.task.timeout=600000 \
    -D mapreduce.reduce.speculative=false \
    -D mapreduce.map.memory.mb=2048 \
    -D mapreduce.reduce.memory.mb=2048 \
    -D yarn.app.mapreduce.am.resource.mb=2048 \
    -D yarn.app.mapreduce.am.command-opts=-Xmx1638m \
    -files "$MAPPER_PATH,$REDUCER_PATH,/app/.venv.tar.gz" \
    -archives /app/.venv.tar.gz#venv \
    -mapper "venv/bin/python $(basename "$MAPPER_PATH")" \
    -reducer "venv/bin/python $(basename "$REDUCER_PATH")" \
    -input "$INPUT_HDFS" \
    -output /index/output \
    -cmdenv "CASSANDRA_HOST=cassandra-server" \
    -cmdenv "PYTHONPATH=venv/lib/python3.8/site-packages"

# Check job status
if [ $? -ne 0 ]; then
    echo "Error: MapReduce job failed"
    exit 1
fi

# Clean up temp files
hadoop fs -rm -r -f "$TMP_HDFS_PATH" 2>/dev/null

echo "Indexing completed successfully!"