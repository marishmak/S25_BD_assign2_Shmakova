#!/bin/bash

# Start SSH server
service ssh restart

# Start required services
bash start-services.sh

# Create virtual environment
python3 -m venv .venv

# Activate virtual environment
source .venv/bin/activate

# Install dependencies
pip install --upgrade pip
pip install -r requirements.txt

# Package virtual environment
if [ -f ".venv.tar.gz" ]; then
    rm .venv.tar.gz
fi
venv-pack -o .venv.tar.gz

# Initialize Cassandra schema using app.py
echo "Initializing Cassandra schema..."
python app.py

# Prepare data (generate synthetic data using PySpark)
echo "Preparing data..."
bash prepare_data.sh

# Run the indexer
bash index.sh

# Run the ranker with a query
bash search.sh "artificial intelligence"