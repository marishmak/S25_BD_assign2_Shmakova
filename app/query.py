#!/usr/bin/env python3
import sys
from pyspark.sql import SparkSession
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import math

def calculate_bm25(N, dl, avgdl, f, qf, df, k1=1.2, b=0.75):
    # BM25 scoring function
    K = k1 * ((1 - b) + b * (dl / avgdl))
    idf = math.log((N - df + 0.5) / (df + 0.5) + 1)
    return idf * (f * (k1 + 1)) / (f + K) * (qf * (k1 + 1)) / (qf + K)

def main():
    # Get query from command line argument
    if len(sys.argv) < 2:
        print("Usage: query.py <query>")
        sys.exit(1)
    
    query = sys.argv[1]
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("DocumentRanker") \
        .config("spark.cassandra.connection.host", "cassandra-server") \
        .getOrCreate()
    
    sc = spark.sparkContext
    
    # Connect to Cassandra
    cluster = Cluster(['cassandra-server'])
    session = cluster.connect('index_keyspace')
    
    # Get total number of documents
    N = session.execute("SELECT COUNT(*) FROM document_index").one()[0]
    
    # Get average document length
    avgdl = session.execute("SELECT AVG(total_term_frequency) FROM bm25_stats").one()[0]
    
    # Tokenize query
    query_terms = re.findall(r'\w+', query.lower())
    
    # Calculate term frequencies in query
    query_term_freq = {}
    for term in query_terms:
        query_term_freq[term] = query_term_freq.get(term, 0) + 1
    
    # Create RDD of query terms
    query_terms_rdd = sc.parallelize(list(query_term_freq.items()))
    
    # For each term, get document stats and calculate BM25
    def process_term(term_qf):
        term, qf = term_qf
        rows = session.execute(
            "SELECT doc_id, term_frequency FROM document_index WHERE term = %s", (term,))
        
        df_row = session.execute(
            "SELECT document_frequency FROM bm25_stats WHERE term = %s", (term,)).one()
        df = df_row[0] if df_row else 0
        
        results = []
        for row in rows:
            doc_id = row.doc_id
            f = row.term_frequency
            
            # Get document length
            dl_row = session.execute(
                "SELECT SUM(term_frequency) FROM document_index WHERE doc_id = %s", (doc_id,)).one()
            dl = dl_row[0] if dl_row else 0
            
            score = calculate_bm25(N, dl, avgdl, f, qf, df)
            results.append((doc_id, score))
        
        return results
    
    scores_rdd = query_terms_rdd.flatMap(process_term)
    
    # Sum scores by document
    doc_scores = scores_rdd.reduceByKey(lambda a, b: a + b)
    
    # Get top 10 documents
    top_docs = doc_scores.takeOrdered(10, key=lambda x: -x[1])
    
    # Get document titles
    for doc_id, score in top_docs:
        title_row = session.execute(
            "SELECT title FROM document_titles WHERE doc_id = %s", (doc_id,)).one()
        title = title_row[0] if title_row else "Unknown"
        print(f"Document ID: {doc_id}, Title: {title}, Score: {score:.4f}")
    
    cluster.shutdown()
    spark.stop()

if __name__ == "__main__":
    main()