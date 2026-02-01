import os
import time
import json
import requests
import boto3
import struct as pystruct 

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.avro.functions import from_avro, to_avro

# Thư viện Kafka Admin
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

# ==========================================
# CẤU HÌNH (CONFIG)
# ==========================================
KAFKA_BOOTSTRAP = "kafka:29092"
SCHEMA_REGISTRY_URL = "http://schema-registry:8081"
INPUT_TOPIC = "raw_clicks_avro"
OUTPUT_TOPIC = "processed_clicks" 

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "datalake"

ITEM_MAP_S3_KEY = "model_registry/item_map.json"
ITEM_CAT_S3_KEY = "model_registry/item_category.json"

# Checkpoint riêng biệt để tránh xung đột
CHECKPOINT_DIR = "s3a://datalake/checkpoints/streaming_v7_stable"

# ==========================================
# ADMIN FUNCTIONS
# ==========================================
def ensure_output_topic_created(topic_name):
    print(f"🛠  Admin: Đang tạo topic '{topic_name}'...")
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP, client_id='spark-admin-setup')
        topic_list = [NewTopic(name=topic_name, num_partitions=1, replication_factor=1)]
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print(f"✅  Admin: Đã tạo '{topic_name}'.")
    except TopicAlreadyExistsError:
        print(f"✅  Admin: Topic '{topic_name}' đã có.")
    except Exception as e:
        print(f"⚠️  Admin Warning: {e}")
    finally:
        try: admin_client.close()
        except: pass

def wait_for_input_topic(spark, topic_name):
    print(f"⏳  Wait: Đang chờ topic Input '{topic_name}'...")
    while True:
        try:
            spark.read.format("kafka") \
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
                .option("subscribe", topic_name) \
                .load().limit(1).collect()
            print(f"✅  Wait: Đã tìm thấy topic '{topic_name}'.")
            return True
        except Exception:
            print(f"💤  Topic '{topic_name}' chưa sẵn sàng. Chờ 5s...")
            time.sleep(5)

# ==========================================
# HELPER FUNCTIONS
# ==========================================
def get_latest_schema(topic_name):
    # Thử lấy schema từ Subject "topic-value"
    url = f"{SCHEMA_REGISTRY_URL}/subjects/{topic_name}-value/versions/latest"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()["schema"]
    except Exception as e:
        pass
    return None

def register_output_schema(topic_name, schema_str):
    url = f"{SCHEMA_REGISTRY_URL}/subjects/{topic_name}-value/versions"
    try:
        response = requests.post(url, json={"schema": schema_str})
        return response.json()["id"]
    except:
        return 1

def load_lookup_maps_from_minio(spark):
    item_map, item_to_cat = {}, {}
    s3_client = boto3.client('s3', endpoint_url=MINIO_ENDPOINT,
                             aws_access_key_id=MINIO_ACCESS_KEY,
                             aws_secret_access_key=MINIO_SECRET_KEY, use_ssl=False)
    try:
        print(f"☁️ Đang tải maps từ MinIO...")
        resp_item = s3_client.get_object(Bucket=MINIO_BUCKET, Key=ITEM_MAP_S3_KEY)
        item_map = {v: int(k) for k, v in json.loads(resp_item['Body'].read()).items()}
        
        resp_cat = s3_client.get_object(Bucket=MINIO_BUCKET, Key=ITEM_CAT_S3_KEY)
        item_to_cat = json.loads(resp_cat['Body'].read())
        print("✅ Tải maps thành công!")
    except Exception as e:
        print(f"⚠️ Dùng map rỗng (Lỗi: {e})")
            
    return (spark.sparkContext.broadcast(item_map), spark.sparkContext.broadcast(item_to_cat))

# ==========================================
# MAIN JOB
# ==========================================
def main():
    spark = SparkSession.builder \
        .appName("Streaming_MinIO_Enrichment_Fix") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # 1. Setup Topic
    ensure_output_topic_created(OUTPUT_TOPIC)
    wait_for_input_topic(spark, INPUT_TOPIC)

    # 2. Load Resources
    bc_item_map, bc_item_to_cat = load_lookup_maps_from_minio(spark)
    
    # 3. 🔥 GET INPUT SCHEMA (VÒNG LẶP CHỜ FIX LỖI NULL POINTER)
    input_avro_schema = None
    print(f"⏳ Đang lấy Schema cho {INPUT_TOPIC}...")
    while input_avro_schema is None:
        input_avro_schema = get_latest_schema(INPUT_TOPIC)
        if input_avro_schema is None:
            print("⚠️ Chưa thấy Schema. Chờ 5s...")
            time.sleep(5)
    print("✅ Đã lấy Schema thành công.")

    # 4. Define Output Schema
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

    # 5. UDFs
    @udf(returnType=IntegerType())
    def lookup_idx(item_id): return bc_item_map.value.get(str(item_id), 0)
    
    @udf(returnType=IntegerType())
    def lookup_cat(item_idx): return int(bc_item_to_cat.value.get(str(item_idx), 1))

    @udf(returnType=BinaryType())
    def add_header(binary_data):
        if binary_data is None: return None
        return pystruct.pack('>bI', 0, output_schema_id) + binary_data

    # 6. Read Stream
    print(f"🚀 Streaming started from {INPUT_TOPIC}...")
    raw_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", INPUT_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    # 7. Process
    # Bỏ 5 bytes header
    avro_payload = expr("substring(value, 6, length(value)-5)")
    
    # from_avro sẽ không bị lỗi nữa vì input_avro_schema đã được đảm bảo khác None
    decoded_df = raw_df.select(from_avro(avro_payload, input_avro_schema).alias("data")).select("data.*")

    processed_df = decoded_df \
        .withColumn("item_idx", lookup_idx(col("item_id"))) \
        .withColumn("category_ids", lookup_cat(col("item_idx"))) \
        .withColumn("processed_at", current_timestamp().cast("long"))

    # 8. Encode & Write
    output_kafka_df = processed_df.select(
        col("user_id").alias("key"),
        add_header(to_avro(struct("*"), output_avro_schema)).alias("value")
    )

    query = output_kafka_df.writeStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("topic", OUTPUT_TOPIC) \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .trigger(processingTime='5 seconds') \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()