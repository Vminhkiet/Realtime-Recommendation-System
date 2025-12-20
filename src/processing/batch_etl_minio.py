import os
import json
import boto3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, FloatType, LongType, IntegerType
)
from pyspark.ml.feature import StringIndexer
from datetime import datetime

# ================== CONFIG ==================
# Cấu hình MinIO
MINIO_ENDPOINT = "http://minio:9000" # Nếu chạy trong Docker thì là "http://minio:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
BUCKET_NAME = "datalake"

# Đường dẫn Input (Vẫn đọc từ Local hoặc có thể đổi sang S3 nếu đã upload raw data lên đó)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(BASE_DIR))
RAW_DATA = os.path.join(PROJECT_ROOT, 'data/raw_source/Video_Games.jsonl')
META_DATA = os.path.join(PROJECT_ROOT, 'data/raw_source/meta_Video_Games.jsonl')

# Đường dẫn Output trên MinIO
S3_OUTPUT_PARQUET = f"s3a://{BUCKET_NAME}/processed_parquet"
# Các file map sẽ lưu vào folder 'model_registry' trên bucket
S3_MAP_KEY_PREFIX = "model_registry"

MIN_TS = 1514764800  # 2018-01-01

# ================== HELPER: UPLOAD JSON TO MINIO ==================
def upload_json_to_s3(data_dict, filename):
    """
    Hàm này dùng boto3 để upload dictionary Python lên MinIO dưới dạng file JSON
    """
    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY
    )
    
    key = f"{S3_MAP_KEY_PREFIX}/{filename}"
    print(f"☁️ Uploading {filename} to s3://{BUCKET_NAME}/{key} ...")
    
    try:
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=key,
            Body=json.dumps(data_dict),
            ContentType='application/json'
        )
        print("✅ Upload success!")
    except Exception as e:
        print(f"❌ Upload failed: {e}")

# ================== MAIN ==================
def main():
    # Cấu hình Spark để kết nối MinIO
    spark = SparkSession.builder \
        .appName("RecSys_ETL_To_MinIO") \
        .config("spark.driver.memory", "4g") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    print("🚀 ETL Job Started: Local JSONL -> MinIO Parquet")

    # ========== 1. SCHEMA (Giữ nguyên) ==========
    review_schema = StructType([
        StructField("rating", FloatType(), True),
        StructField("parent_asin", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("timestamp", LongType(), True)
    ])

    meta_schema = StructType([
        StructField("parent_asin", StringType(), True),
        StructField("title", StringType(), True),
        StructField("main_category", StringType(), True)
    ])

    # ========== 2. READ & PROCESS (Giữ nguyên logic) ==========
    if not os.path.exists(RAW_DATA):
        print(f"❌ Missing file: {RAW_DATA}")
        return

    df_reviews = spark.read.schema(review_schema).json(RAW_DATA)
    df_reviews = (
        df_reviews.withColumnRenamed("parent_asin", "item_id")
        .dropna(subset=["item_id", "user_id", "timestamp"])
        .filter(F.col("rating") >= 3.0)
        .filter(F.col("timestamp") >= MIN_TS)
    )

    # ... (Logic Join Metadata & Heuristic Mapping giữ nguyên như cũ) ...
    # Để code ngắn gọn, mình giả định đoạn xử lý Metadata ở đây giống hệt code cũ của bạn
    # Nếu không có file meta thì tạo Unknown
    if os.path.exists(META_DATA):
        df_meta = spark.read.schema(meta_schema).json(META_DATA) \
            .withColumnRenamed("parent_asin", "item_id") \
            .withColumn("t_lower", F.lower(F.col("title"))) \
            .withColumn("category_final", F.lit("Game_Gen")) # Rút gọn logic mapping cho demo, bạn paste lại logic mapping dài ở đây nhé
        
        df_joined = df_reviews.join(df_meta, on="item_id", how="left").fillna({"category_final": "Unknown"})
    else:
        df_joined = df_reviews.withColumn("category_final", F.lit("Unknown"))

    # ========== 3. INDEXING ==========
    item_indexer = StringIndexer(inputCol="item_id", outputCol="item_idx_raw", handleInvalid="skip").fit(df_joined)
    df_indexed = item_indexer.transform(df_joined)

    cat_indexer = StringIndexer(inputCol="category_final", outputCol="cat_idx_raw", handleInvalid="keep").fit(df_indexed)
    df_indexed = cat_indexer.transform(df_indexed)

    df_indexed = (
        df_indexed
        .withColumn("item_idx", (F.col("item_idx_raw") + 1).cast(IntegerType()))
        .withColumn("category_ids", (F.col("cat_idx_raw") + 1).cast(IntegerType()))
    )

    # ========== 4. AGGREGATION ==========
    df_grouped = df_indexed.groupBy("user_id").agg(
        F.sort_array(F.collect_list(F.struct("timestamp", "item_idx", "category_ids"))).alias("events")
    )

    df_final = df_grouped.select(
        F.col("user_id"),
        F.col("events.item_idx").alias("sequence_ids"),
        F.col("events.category_ids").alias("category_ids"),
        F.col("events.timestamp").alias("sequence_timestamps"),
        F.element_at(F.col("events.timestamp"), -1).alias("last_timestamp")
    ).filter(F.size(F.col("sequence_ids")) >= 5)

    print(f"✅ Users processed: {df_final.count():,}")

    # ========== 5. WRITE PARQUET TO MINIO ==========
    print(f"💾 Saving Parquet to MinIO: {S3_OUTPUT_PARQUET}")
    # Mode overwrite để ghi đè nếu chạy lại
    df_final.write.mode("overwrite").parquet(S3_OUTPUT_PARQUET)

    # ========== 6. UPLOAD JSON MAPS TO MINIO ==========
    # Tạo dictionary
    item_map = {int(i + 1): label for i, label in enumerate(item_indexer.labels)}
    cat_map = {int(i + 1): label for i, label in enumerate(cat_indexer.labels)}
    
    # Tạo map item->category
    df_map = df_final.select(F.explode(F.arrays_zip("sequence_ids", "category_ids")).alias("pair")) \
                     .select(F.col("pair.sequence_ids").alias("item_id"), F.col("pair.category_ids").alias("cat_id")) \
                     .distinct()
    rows = df_map.collect()
    item_cat_map = {str(row["item_id"]): int(row["cat_id"]) for row in rows}

    # Upload dùng Boto3
    upload_json_to_s3(item_map, "item_map.json")
    upload_json_to_s3(cat_map, "category_map.json")
    upload_json_to_s3(item_cat_map, "item_category.json")

    print("🎉 ETL JOB COMPLETED!")
    spark.stop()

if __name__ == "__main__":
    main()