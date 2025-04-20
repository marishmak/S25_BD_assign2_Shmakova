#!/usr/bin/env python3
import sys
import os
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def connect_to_cassandra(hosts, max_retries=5, retry_delay=5):
    """Connect to Cassandra with retries"""
    for attempt in range(max_retries):
        try:
            cluster = Cluster(hosts)
            session = cluster.connect('index_keyspace')
            logger.info("Connected to Cassandra successfully")
            return cluster, session
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"Connection attempt {attempt + 1} failed: {str(e)}. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                logger.error(f"Failed to connect to Cassandra after {max_retries} attempts")
                raise

def process_term(session, term, doc_list):
    """Process a term and its documents"""
    try:
        # Calculate statistics
        doc_frequency = len(doc_list)
        total_term_freq = sum(freq for _, freq in doc_list)
        
        # Prepare batch for vocabulary and stats
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        
        # Insert into vocabulary
        doc_ids = [doc_id for doc_id, _ in doc_list]
        batch.add(
            session.prepare("INSERT INTO vocabulary (term, doc_ids) VALUES (?, ?)"),
            (term, doc_ids)
        )
        
        # Insert into bm25_stats
        batch.add(
            session.prepare("""
                INSERT INTO bm25_stats (term, document_frequency, total_term_frequency) 
                VALUES (?, ?, ?)
            """),
            (term, doc_frequency, total_term_freq)
        )
        
        # Execute batch
        session.execute(batch)
        
        # Insert document index entries in smaller batches
        doc_batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        insert_stmt = session.prepare("""
            INSERT INTO document_index (term, doc_id, term_frequency) 
            VALUES (?, ?, ?)
        """)
        
        for doc_id, freq in doc_list:
            doc_batch.add(insert_stmt, (term, doc_id, freq))
            if len(doc_batch) >= 50:  # Batch size of 50
                session.execute(doc_batch)
                doc_batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        
        if len(doc_batch) > 0:
            session.execute(doc_batch)
            
    except Exception as e:
        logger.error(f"Error processing term {term}: {str(e)}")
        raise

def main():
    # Get Cassandra host from environment or use default
    cassandra_host = os.getenv('CASSANDRA_HOST', 'cassandra-server')
    
    # Connect to Cassandra
    try:
        cluster, session = connect_to_cassandra([cassandra_host])
    except Exception as e:
        logger.error(f"Failed to connect to Cassandra: {str(e)}")
        sys.exit(1)

    current_term = None
    doc_list = []
    
    try:
        for line in sys.stdin:
            try:
                term, doc_id, freq = line.strip().split('\t')
                freq = int(freq)
                
                if term != current_term:
                    if current_term is not None:
                        process_term(session, current_term, doc_list)
                    current_term = term
                    doc_list = []
                
                doc_list.append((doc_id, freq))
                
            except Exception as e:
                logger.error(f"Error processing line: {str(e)}")
                continue
        
        # Process the last term
        if current_term is not None:
            process_term(session, current_term, doc_list)
            
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        sys.exit(1)
    finally:
        cluster.shutdown()

if __name__ == "__main__":
    main()