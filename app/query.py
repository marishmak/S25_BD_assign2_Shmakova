#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys

# BM25 parameters
k1 = 1.5
b = 0.75

# Initialize Spark session
spark = SparkSession.builder \
    .appName("BM25Search") \
    .config("spark.cassandra.connection.host", "cassandra-host") \
    .getOrCreate()

sc = spark.sparkContext

# Read user query from stdin
query = sys.stdin.read().strip().lower()
query_terms = query.split()

# Load Cassandra tables into DataFrames
vocabulary_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="vocabulary", keyspace="index_keyspace") \
    .load()

document_index_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="document_index", keyspace="index_keyspace") \
    .load()

bm25_stats_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="bm25_stats", keyspace="index_keyspace") \
    .load()

# Broadcast BM25 statistics for efficiency
avg_doc_length = bm25_stats_df.selectExpr("sum(total_term_frequency) / count(term)").collect()[0][0]
total_docs = bm25_stats_df.count()

# Filter vocabulary to only include query terms
query_vocabulary_df = vocabulary_df.filter(col("term").isin(query_terms))

# Join with document index to get term frequencies for query terms
query_term_frequencies_df = query_vocabulary_df.join(
    document_index_df,
    query_vocabulary_df.term == document_index_df.term
).select(
    document_index_df.term,
    document_index_df.doc_id,
    document_index_df.term_frequency
)

# Broadcast BM25 stats for query terms
query_bm25_stats_df = bm25_stats_df.filter(col("term").isin(query_terms))
query_bm25_stats_rdd = query_bm25_stats_df.rdd.collectAsMap()
query_bm25_stats_broadcast = sc.broadcast(query_bm25_stats_rdd)

# Calculate BM25 scores
def calculate_bm25_score(row):
    term = row["term"]
    doc_id = row["doc_id"]
    term_freq = row["term_frequency"]

    stats = query_bm25_stats_broadcast.value[term]
    doc_freq = stats["document_frequency"]
    idf = total_docs / doc_freq
    numerator = term_freq * (k1 + 1)
    denominator = term_freq + k1 * (1 - b + b * avg_doc_length)
    score = idf * (numerator / denominator)

    return (doc_id, score)

bm25_scores_rdd = query_term_frequencies_df.rdd.map(calculate_bm25_score)

# Aggregate scores per document
top_documents = bm25_scores_rdd.reduceByKey(lambda x, y: x + y) \
    .sortBy(lambda x: x[1], ascending=False) \
    .take(10)

# Output top 10 documents
for doc_id, score in top_documents:
    print(f"Document ID: {doc_id}, Score: {score}")