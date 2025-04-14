from pyspark.sql import SparkSession
from tqdm import tqdm

spark = SparkSession.builder \
    .appName('data preparation') \
    .master("local") \
    .config("spark.sql.parquet.enableVectorizedReader", "true") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

try:
    # Define HDFS path for the Parquet file
    hdfs_parquet_path = "/index/data/a.parquet"

    # Read Parquet file using Spark's built-in reader
    print("Reading Parquet file...")
    df = spark.read.parquet(hdfs_parquet_path)

except Exception as e:
    print(f"Error reading Parquet file: {e}")
    spark.stop()
    exit(1)

# Select required columns and sample 100 documents
n = 11
print("Sampling documents...")
df_sample = df.select(['id', 'title', 'text']).sample(fraction=100 * n / df.count(), seed=0).limit(n)

# Define function to create text files with progress tracking
def create_doc(row):
    try:
        filename = f"/app/data/{row['id']}_{row['title'].replace(' ', '_')}.txt"
        with open(filename, "w") as f:
            f.write(row['text'])
    except Exception as e:
        print(f"Error creating document {row['id']}: {e}")

# Use tqdm to track progress while creating files
print("Creating text files...")
for row in tqdm(df_sample.collect(), desc="Processing documents", unit="doc"):
    create_doc(row)

print("Data preparation completed successfully!")
spark.stop()