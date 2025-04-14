#!/bin/bash
echo "Waiting for Cassandra to start..."
until cqlsh -e "describe keyspaces"; do
    sleep 5
done

echo "Initializing Cassandra schema..."
cqlsh -e "
CREATE KEYSPACE IF NOT EXISTS index_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE index_keyspace;

CREATE TABLE IF NOT EXISTS vocabulary (
    term text PRIMARY KEY,
    doc_ids list<text>
);

CREATE TABLE IF NOT EXISTS document_index (
    term text,
    doc_id text,
    term_frequency int,
    PRIMARY KEY (term, doc_id)
);

CREATE TABLE IF NOT EXISTS bm25_stats (
    term text PRIMARY KEY,
    document_frequency int,
    total_term_frequency int
);
"
echo "Cassandra schema initialized successfully."