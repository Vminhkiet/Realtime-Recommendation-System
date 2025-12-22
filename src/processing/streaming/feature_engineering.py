import os
import json
import requests
import boto3
import struct as pystruct 

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.avro.functions import from_avro, to_avro

# ==========================================
# CẤU HÌNH (CONFIG)
# ==========================================
KAFKA_BOOTSTRAP = "kafka:29092"
SCHEMA_REGISTRY_URL = "http://schema-registry:8081"
INPUT_TOPIC = "raw_clicks_avro"
OUTPUT_TOPIC = "processed_clicks" 

# MinIO Config
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "datalake"

# S3 Keys cho các file Mapping
ITEM_MAP_S3_KEY = "model_registry/item_map.json"
ITEM_CAT_S3_KEY = "model_registry/item_category.json"

CHECKPOINT_DIR = "s3a://datalake/checkpoints/streaming_v6"

# ==========================================
# HELPER FUNCTIONS
# ==========================================
def get_latest_schema(topic_name):
    """Lấy Avro Schema mới nhất từ Schema Registry"""
    url = f"{SCHEMA_REGISTRY_URL}/subjects/{topic_name}-value/versions/latest"
    try:
        response = requests.get(url)
        return response.json()["schema"] if response.status_code == 200 else None
    except Exception as e:
        print(f"❌ Lỗi lấy Schema: {e}")
        return None

def register_output_schema(topic_name, schema_str):
    """Đăng ký Schema mới cho Output Topic"""
    url = f"{SCHEMA_REGISTRY_URL}/subjects/{topic_name}-value/versions"
    response = requests.post(url, json={"schema": schema_str})
    return response.json()["id"]

def load_lookup_maps_from_minio(spark):
    """Tải Item Map và Category Map trực tiếp từ MinIO vào RAM"""
    item_map = {}
    item_to_cat = {}
    
    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        use_ssl=False
    )
    
    try:
        # 1. Tải item_map.json (ASIN -> Index)
        print(f"☁️ Đang tải {ITEM_MAP_S3_KEY}...")
        resp_item = s3_client.get_object(Bucket=MINIO_BUCKET, Key=ITEM_MAP_S3_KEY)
        raw_item = json.loads(resp_item['Body'].read().decode('utf-8'))
        # Đảo ngược map: {"1": "B000X"} -> {"B000X": 1}
        item_map = {v: int(k) for k, v in raw_item.items()}
            
        # 2. Tải item_category.json (Index -> Category Index)
        print(f"☁️ Đang tải {ITEM_CAT_S3_KEY}...")
        resp_cat = s3_client.get_object(Bucket=MINIO_BUCKET, Key=ITEM_CAT_S3_KEY)
        item_to_cat = json.loads(resp_cat['Body'].read().decode('utf-8'))
        
        print("✅ Tải bản đồ từ MinIO thành công!")
    except Exception as e:
        print(f"❌ Lỗi tải bản đồ từ MinIO: {e}")
            
    return (spark.sparkContext.broadcast(item_map), 
            spark.sparkContext.broadcast(item_to_cat))

# ==========================================
# MAIN JOB
# ==========================================
def main():
    spark = SparkSession.builder \
        .appName("Streaming_MinIO_Enrichment") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")

    # Load dữ liệu tra cứu từ MinIO
    bc_item_map, bc_item_to_cat = load_lookup_maps_from_minio(spark)
    input_avro_schema = get_latest_schema(INPUT_TOPIC)
    
    # Định nghĩa Schema đầu ra có thêm category_ids
    output_avro_schema = """
    {
      "type": "record",
      "name": "ProcessedClick",
      "fields": [
        {"name": "event_id", "type": ["null", "string"], "default": null},
        {"name": "session_id", "type": ["null", "string"], "default": null},
        {"name": "item_id", "type": ["null", "string"], "default": null},
        {"name": "user_id", "type": ["null", "string"], "default": null},
        {"name": "event_type", "type": ["null", "string"], "default": null},
        {"name": "rating_original", "type": ["null", "float"], "default": null},
        {"name": "timestamp", "type": ["null", "string"], "default": null},
        {"name": "context", "type": ["null", {
                "type": "record", "name": "ContextStruct",
                "fields": [
                    {"name": "device", "type": ["null", "string"], "default": null},
                    {"name": "location", "type": ["null", "string"], "default": null},
                    {"name": "ip", "type": ["null", "string"], "default": null}
                ]
            }], "default": null},
        {"name": "item_idx", "type": "int"},
        {"name": "category_ids", "type": "int"},
        {"name": "processed_at", "type": "long"}
      ]
    }
    """
    output_schema_id = register_output_schema(OUTPUT_TOPIC, output_avro_schema)

    # --- UDFs Logic ---
    @udf(returnType=IntegerType())
    def lookup_idx(item_id):
        # Mặc định là 0 (Padding)
        return bc_item_map.value.get(str(item_id), 0)
    
    @udf(returnType=IntegerType())
    def lookup_cat(item_idx):
        # Mặc định là 1 (Other_Gaming)
        return int(bc_item_to_cat.value.get(str(item_idx), 1))

    @udf(returnType=BinaryType())
    def add_header(binary_data):
        if binary_data is None: return None
        # Đóng gói magic byte 0 và schema_id 4 bytes
        return pystruct.pack('>bI', 0, output_schema_id) + binary_data

    # --- Read Stream ---
    raw_df = spark.readStream.format("kafka") \
        .option("failOnDataLoss", "false")\
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", INPUT_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    # Skip 5 bytes header của Confluent Avro
    avro_payload = expr("substring(value, 6, length(value)-5)")
    decoded_df = raw_df.select(from_avro(avro_payload, input_avro_schema).alias("data")).select("data.*")

    # 🔥 TRANSFORM: Làm giàu dữ liệu với Category
    processed_df = decoded_df \
        .withColumn("item_idx", lookup_idx(col("item_id"))) \
        .withColumn("category_ids", lookup_cat(col("item_idx"))) \
        .withColumn("processed_at", current_timestamp().cast("long"))

    # Encode sang Avro và thêm Header
    output_kafka_df = processed_df.select(
        col("user_id").alias("key"),
        add_header(to_avro(struct("*"), output_avro_schema)).alias("value")
    )

    # --- Write Stream ---
    print(f"🚀 Streaming trơn tru: {INPUT_TOPIC} -> {OUTPUT_TOPIC} ...")
    query = output_kafka_df.writeStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("topic", OUTPUT_TOPIC) \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()