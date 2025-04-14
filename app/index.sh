#!/bin/bash

# Default HDFS input directory
HDFS_INPUT_DIR="/index/data"

# Check if a local file is provided as an argument
if [ "$1" ]; then
    LOCAL_FILE=$1

    # Create a temporary HDFS directory for the input file
    echo "Creating temporary HDFS input directory..."
    hdfs dfs -mkdir -p /tmp/index/input || { echo "Error: Failed to create HDFS input directory."; exit 1; }

    # Copy the local file to HDFS
    echo "Copying local file '$LOCAL_FILE' to HDFS..."
    hdfs dfs -put "$LOCAL_FILE" /tmp/index/input || { echo "Error: Failed to copy local file to HDFS."; exit 1; }

    # Set the input path to the temporary HDFS directory
    INPUT_PATH="/tmp/index/input"
else
    # Use the default HDFS input directory if no local file is provided
    INPUT_PATH=$HDFS_INPUT_DIR
fi

# Verify the input path exists in HDFS
echo "Verifying input path '$INPUT_PATH'..."
if ! hdfs dfs -test -d "$INPUT_PATH"; then
    echo "Error: Input path '$INPUT_PATH' not found in HDFS."
    exit 1
fi

# Define the output directory in HDFS
OUTPUT_DIR="/tmp/index/output"

# Remove the output directory if it already exists
echo "Cleaning up previous output directory..."
if hdfs dfs -test -d "$OUTPUT_DIR"; then
    hdfs dfs -rm -r "$OUTPUT_DIR" || { echo "Warning: Failed to remove previous output directory."; exit 1; }
fi

# Activate the virtual environment
echo "Activating virtual environment..."
source .venv/bin/activate || { echo "Error: Failed to activate virtual environment."; exit 1; }

# Get the Python interpreter path
PYTHON_PATH=$(which python3)
if [ -z "$PYTHON_PATH" ]; then
    echo "Error: Python3 interpreter not found."
    exit 1
fi

# Define paths to the mapper and reducer scripts
MAPPER_PATH="$(pwd)/mapreduce/mapper1.py"
REDUCER_PATH="$(pwd)/mapreduce/reducer1.py"

# Verify the existence of the mapper and reducer scripts
if [ ! -f "$MAPPER_PATH" ] || [ ! -f "$REDUCER_PATH" ]; then
    echo "Error: Mapper or reducer script not found."
    exit 1
fi

# Define the path to the Hadoop Streaming JAR
HADOOP_STREAMING_JAR_PATH="/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar"

# Download the Hadoop Streaming JAR if it is missing
if [ ! -f "$HADOOP_STREAMING_JAR_PATH" ]; then
    echo "Downloading Hadoop Streaming JAR..."
    mkdir -p "/usr/local/hadoop/share/hadoop/tools/lib" || { echo "Error: Failed to create directory for Hadoop Streaming JAR."; exit 1; }
    wget -O "$HADOOP_STREAMING_JAR_PATH" "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-streaming/3.4.1/hadoop-streaming-3.4.1.jar" || { echo "Error: Failed to download Hadoop Streaming JAR."; exit 1; }
fi

# Verify the existence of the Hadoop Streaming JAR
if [ ! -f "$HADOOP_STREAMING_JAR_PATH" ]; then
    echo "Error: Hadoop Streaming JAR not found at '$HADOOP_STREAMING_JAR_PATH'."
    exit 1
fi

# Check and update commons-cli if necessary
COMMONS_CLI_PATH="/usr/local/hadoop/share/hadoop/common/lib/commons-cli-1.4.jar"
if [ ! -f "$COMMONS_CLI_PATH" ]; then
    echo "Updating commons-cli to version 1.4..."
    wget -O /tmp/commons-cli-1.4.jar "https://repo1.maven.org/maven2/commons-cli/commons-cli/1.4/commons-cli-1.4.jar" || { echo "Error: Failed to download commons-cli."; exit 1; }
    mv /tmp/commons-cli-1.4.jar /usr/local/hadoop/share/hadoop/common/lib/commons-cli-1.4.jar || { echo "Error: Failed to replace commons-cli."; exit 1; }
    rm -f /usr/local/hadoop/share/hadoop/common/lib/commons-cli-1.2.jar || true
fi

# Run the Hadoop Streaming job
echo "Running Hadoop Streaming job..."
hadoop jar "$HADOOP_STREAMING_JAR_PATH" \
    -input "$INPUT_PATH" \
    -output "$OUTPUT_DIR" \
    -mapper "$PYTHON_PATH $MAPPER_PATH" \
    -reducer "$PYTHON_PATH $REDUCER_PATH" || { echo "Error: Hadoop Streaming job failed."; exit 1; }

# Verify the output directory was created
echo "Verifying output directory..."
if hdfs dfs -test -d "$OUTPUT_DIR"; then
    echo "Hadoop Streaming job completed successfully. Output is available at '$OUTPUT_DIR'."
else
    echo "Error: Output directory '$OUTPUT_DIR' not found."
    exit 1
fi