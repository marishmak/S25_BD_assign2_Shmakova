from pyspark.sql import SparkSession
from tqdm import tqdm
import os
import re
import time

def create_spark_session():
    return SparkSession.builder \
        .appName('data-preparation') \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://cluster-master:9000") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .config("spark.hadoop.dfs.namenode.rpc-address", "cluster-master:9000") \
        .config("spark.hadoop.dfs.client.failover.proxy.provider.mycluster", 
               "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider") \
        .config("spark.sql.parquet.enableVectorizedReader", "true") \
        .config("spark.network.timeout", "600s") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .getOrCreate()

def read_parquet_file(spark, path):
    max_retries = 3
    retry_delay = 5  # seconds
    
    for attempt in range(max_retries):
        try:
            print(f"Reading from: {path} (attempt {attempt + 1})")
            df = spark.read.parquet(path).select(['id', 'title', 'text'])
            count = df.count()
            print(f"Successfully read {count} records")
            return df
        except Exception as e:
            print(f"Error reading Parquet (attempt {attempt + 1}): {str(e)}")
            if attempt < max_retries - 1:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2  # exponential backoff
            else:
                return None

def clean_filename(title):
    cleaned = re.sub(r'[^\w\s-]', '', title).strip().replace(' ', '_')
    cleaned = re.sub(r'_+', '_', cleaned)
    return cleaned


def get_exact_sample(df, n=1000):
    try:
        # First try to get exact n samples without replacement
        fraction = n / df.count()
        sampled = df.sample(withReplacement=False, fraction=fraction, seed=42)
        
        # If we didn't get enough, sample with replacement for remaining
        if sampled.count() < n:
            needed = n - sampled.count()
            additional = df.sample(withReplacement=True, fraction=1.0, seed=42).limit(needed)
            sampled = sampled.union(additional)
        
        return sampled.limit(n)
    except Exception as e:
        print(f"Sampling error: {str(e)}")
        return None

def save_documents(df_sample, output_dir="/app/data"):
    try:
        os.makedirs(output_dir, exist_ok=True)
        count = df_sample.count()
        print(f"Saving {count} documents to {output_dir}")
        
        # Process in batches to avoid memory issues
        batch_size = 50
        for i in tqdm(range(0, count, batch_size), desc="Processing documents"):
            batch = df_sample.limit(batch_size).collect()
            df_sample = df_sample.subtract(spark.createDataFrame(batch))
            
            for row in batch:
                try:
                    clean_title = clean_filename(row['title'])
                    filename = f"{output_dir}/{row['id']}_{clean_title}.txt"
                    
                    with open(filename, "w", encoding='utf-8') as f:
                        f.write(row['text'])
                except Exception as e:
                    print(f"Error saving document {row['id']}: {str(e)}")
    except Exception as e:
        print(f"Error saving documents: {str(e)}")
        raise

spark = None
try:
    spark = create_spark_session()
    hdfs_path = "hdfs://cluster-master:9000/index/data/a.parquet"
    
    # Read data with timeout
    df = read_parquet_file(spark, hdfs_path)
    if not df:
        raise Exception("Failed to read Parquet file")
    
    # Get exact 1000 samples
    df_sample = get_exact_sample(df, 1000)
    if not df_sample or df_sample.count() < 1000:
        raise Exception("Failed to get 1000 samples")
    
    # Save documents
    save_documents(df_sample)
    
    print("Data preparation completed successfully!")
    
    
except Exception as e:
    print(f"Fatal error: {str(e)}")
    

