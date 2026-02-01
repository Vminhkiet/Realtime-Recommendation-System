import os
import json
import boto3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    FloatType, LongType, IntegerType, ArrayType
)
from pyspark.ml.feature import StringIndexer
from datetime import datetime, timedelta

# ================== CONFIG (BATCH PRODUCTION) ==================
MINIO_ENDPOINT = "http://minio:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
BUCKET_NAME = "datalake"

# INPUT:
# 1. Raw Logs: In a real batch scenario, you read accumulated small parquet files from streaming
#    Here we use a wildcard to read all partitions.
#    If you are still loading the initial dataset, point this to your raw JSONL path.
INPUT_SOURCE = f"s3a://{BUCKET_NAME}/topics/processed_clicks/*/*/*/*" 

# 2. Metadata: Usually static, can be read from local volume or S3
META_DATA_PATH = "/home/spark/work/data/raw_source/meta_Video_Games.jsonl"

# OUTPUT:
# Versioned output folder for reproducible training
current_week = datetime.now().strftime("%Y_week_%U")
S3_OUTPUT_TRAIN = f"s3a://{BUCKET_NAME}/training_data/{current_week}"
S3_MAP_PREFIX = "model_registry"

# ================== HELPER: UPLOAD TO MINIO ==================
def upload_json_to_s3(data_dict, filename):
    """
    Uploads a Python dictionary as a JSON file to MinIO using boto3.
    """
    s3_client = boto3.client('s3', endpoint_url=MINIO_ENDPOINT,
                             aws_access_key_id=ACCESS_KEY,
                             aws_secret_access_key=SECRET_KEY, use_ssl=False)
    key = f"{S3_MAP_PREFIX}/{filename}"
    try:
        s3_client.put_object(Bucket=BUCKET_NAME, Key=key,
                             Body=json.dumps(data_dict), ContentType='application/json')
        print(f"✅ Uploaded map to MinIO: {key}")
    except Exception as e:
        print(f"❌ Failed to upload {filename}: {e}")

# ================== MAIN BATCH JOB ==================
def main():
    spark = SparkSession.builder \
        .appName(f"Batch_ETL_{current_week}") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    print(f"🚀 Starting Batch ETL for week: {current_week}")

    # ================= 1. READ & FILTER DATA =================
    # Reading accumulated Parquet files from Streaming (or raw logs)
    # Using mergeSchema=true is safer if schemas evolved over time
    print(f"📂 Reading logs from: {INPUT_SOURCE}")
    try:
        df_raw = spark.read.option("mergeSchema", "true").parquet(INPUT_SOURCE)
        
        # Filter: Only keep data from the last 30 days (Recency bias for RecSys)
        # Assuming 'timestamp' is in milliseconds
        thirty_days_ms = (datetime.now() - timedelta(days=30)).timestamp() * 1000
        df_logs = df_raw.filter(F.col("timestamp") >= thirty_days_ms)
        print(f"📉 Filtered logs count: {df_logs.count()}")
        
    except Exception as e:
        print(f"⚠️ Could not read from S3 ({e}). Falling back to local raw for demo.")
        # Fallback for initial load
        RAW_LOCAL = "/home/spark/work/data/raw_source/Video_Games.jsonl"
        df_logs = spark.read.json(RAW_LOCAL).withColumnRenamed("parent_asin", "item_id") \
            .filter(F.col("rating") >= 3.0)

    # ================= 2. ENRICH WITH METADATA =================
    print("📘 Processing Metadata...")
    meta_schema = StructType([
        StructField("parent_asin", StringType(), True),
        StructField("title", StringType(), True),
        StructField("categories", ArrayType(StringType()), True)
    ])
    
    if os.path.exists(META_DATA_PATH):
        df_meta = spark.read.schema(meta_schema).json(META_DATA_PATH) \
            .withColumnRenamed("parent_asin", "item_id")
        
        # Improved Heuristic Mapping (Array + Title)
        df_meta = df_meta.withColumn("cat_str", F.lower(F.concat_ws(" ", F.col("categories")))) \
                         .withColumn("t_lower", F.lower(F.col("title"))) \
                         .withColumn("category_final", 
                            F.when(F.col("cat_str").contains("pc") | F.col("t_lower").contains("pc"), "PC_Gaming")
                            .when(F.col("cat_str").contains("ps5") | F.col("t_lower").contains("ps5"), "PS5")
                            .when(F.col("cat_str").contains("ps4") | F.col("t_lower").contains("ps4"), "PS4")
                            .when(F.col("cat_str").contains("xbox"), "Xbox")
                            .when(F.col("cat_str").contains("switch") | F.col("t_lower").contains("switch"), "Nintendo")
                            .when(F.col("cat_str").contains("accessory") | F.col("t_lower").contains("controller"), "Accessories")
                            .otherwise(F.coalesce(F.element_at(F.col("categories"), 2), F.lit("Other_Gaming")))
                         )
        
        # Join logs with enriched metadata
        # Note: We select distinct item_ids to avoid explosion during join
        df_meta_unique = df_meta.select("item_id", "category_final").dropDuplicates(["item_id"])
        
        df_enriched = df_logs.join(df_meta_unique, on="item_id", how="left") \
                             .fillna({"category_final": "General_Gaming"})
    else:
        print("⚠️ Metadata not found, using default category.")
        df_enriched = df_logs.withColumn("category_final", F.lit("Unknown"))

    # ================= 3. INDEXING =================
    print("🔢 Indexing items and categories...")
    
    # StringIndexer is crucial for SasRec (IDs must be 1..N)
    item_indexer = StringIndexer(inputCol="item_id", outputCol="item_idx_raw").fit(df_enriched)
    cat_indexer = StringIndexer(inputCol="category_final", outputCol="cat_idx_raw").fit(df_enriched)

    df_indexed = item_indexer.transform(df_enriched)
    df_indexed = cat_indexer.transform(df_indexed)

    # Shift indices by +1 to reserve 0 for padding
    df_ready = df_indexed.withColumn("item_idx", (F.col("item_idx_raw") + 1).cast("int")) \
                         .withColumn("category_ids", (F.col("cat_idx_raw") + 1).cast("int"))

    # ================= 4. COMPACTION & AGGREGATION =================
    print("📦 Building User Sequences (Compaction)...")
    
    # This step effectively "compacts" thousands of small streaming records into user sessions
    df_final = df_ready.groupBy("user_id").agg(
        F.sort_array(F.collect_list(F.struct("timestamp", "item_idx", "category_ids"))).alias("events")
    ).select(
        F.col("user_id"),
        # Slice to keep sequence length manageable (e.g., last 50 interactions)
        F.slice(F.col("events.item_idx"), -50, 50).alias("sequence_ids"),
        F.slice(F.col("events.category_ids"), -50, 50).alias("category_ids"),
        F.element_at(F.col("events.timestamp"), -1).alias("last_timestamp")
    ).filter(F.size(F.col("sequence_ids")) >= 5) # Filter out short sessions

    # ================= 5. WRITE TO MINIO (PARQUET) =================
    print(f"💾 Saving Training Data to: {S3_OUTPUT_TRAIN}")
    
    # coalesce(1) combines all data into 1 single file per partition. 
    # Critical for avoiding "small file problem" in training.
    df_final.coalesce(1).write.mode("overwrite").parquet(S3_OUTPUT_TRAIN)

    # ================= 6. GENERATE & UPLOAD MAPS =================
    print("🗺️ Generating Lookup Maps...")
    
    # 1. Item ID Map (Model ID -> Real ID)
    item_map = {int(i + 1): label for i, label in enumerate(item_indexer.labels)}
    
    # 2. Category Map (Model ID -> Category Name)
    cat_map = {int(i + 1): label for i, label in enumerate(cat_indexer.labels)}
    
    # 3. Item -> Category Map (For Real-time Inference Enrichment)
    # We extract distinct pairs of (item_idx, category_id) from the processed data
    df_ic_map = df_ready.select("item_idx", "category_ids").distinct()
    # Collect to driver (careful if millions of items, but okay for typical e-commerce)
    rows = df_ic_map.collect()
    item_cat_map = {str(row["item_idx"]): int(row["category_ids"]) for row in rows}

    # Upload using Boto3
    upload_json_to_s3(item_map, "item_map.json")
    upload_json_to_s3(cat_map, "category_map.json")
    upload_json_to_s3(item_cat_map, "item_category.json")

    print(f"🎉 Batch ETL Completed! Output: {S3_OUTPUT_TRAIN}")
    spark.stop()

if __name__ == "__main__":
    main()