import os
import json
import requests
# 🔥 [CHANGE] Đổi tên struct thành pystruct để không bị Spark ghi đè
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

# Đổi checkpoint v4 để chạy mới
CHECKPOINT_DIR = "s3a://datalake/checkpoints/streaming_inference_avro_v4"
ITEM_MAP_PATH = "/home/spark/work/data/model_registry/item_map.json"

# ==========================================
# HELPER FUNCTIONS
# ==========================================
def get_latest_schema(topic_name):
    subject = f"{topic_name}-value"
    url = f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions/latest"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()["schema"]
        else:
            raise Exception(f"Lỗi API: {response.text}")
    except Exception as e:
        print(f"❌ Không lấy được Schema Input: {e}")
        raise e

def register_output_schema(topic_name, schema_str):
    subject = f"{topic_name}-value"
    url = f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions"
    payload = {"schema": schema_str}
    
    print(f"🌍 Đăng ký Schema Output lên: {url}")
    try:
        response = requests.post(url, json=payload)
        if response.status_code == 200:
            schema_id = response.json()["id"]
            print(f"✅ Schema ID cho Output là: {schema_id}")
            return schema_id
        else:
            raise Exception(f"Lỗi đăng ký Schema: {response.text}")
    except Exception as e:
        print(f"❌ Lỗi Fatal: {e}")
        raise e

def load_lookup_map(spark):
    if os.path.exists(ITEM_MAP_PATH):
        with open(ITEM_MAP_PATH, 'r') as f:
            raw_map = json.load(f)
            item_map = {v: int(k) for k, v in raw_map.items()}
    else:
        item_map = {}
    return spark.sparkContext.broadcast(item_map)

# ==========================================
# MAIN JOB
# ==========================================
def main():
    print("🚀 Khởi động Spark Streaming (Avro Chuẩn Confluent)...")

    spark = SparkSession.builder \
        .appName("Streaming_Inference_Processor") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    bc_item_map = load_lookup_map(spark)
    input_avro_schema = get_latest_schema(INPUT_TOPIC)
    
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
        {
            "name": "context",
            "type": ["null", {
                "type": "record",
                "name": "ContextStruct",
                "fields": [
                    {"name": "device", "type": ["null", "string"], "default": null},
                    {"name": "location", "type": ["null", "string"], "default": null},
                    {"name": "ip", "type": ["null", "string"], "default": null}
                ]
            }],
            "default": null
        },
        {"name": "item_idx", "type": "int"},
        {"name": "processed_at", "type": "long"}
      ]
    }
    """
    
    output_schema_id = register_output_schema(OUTPUT_TOPIC, output_avro_schema)

    def lookup_idx(item_id):
        return bc_item_map.value.get(str(item_id), 0)
    lookup_udf = udf(lookup_idx, IntegerType())

    def get_schema_id(topic_name):
        # Subject mặc định là tên-topic-value
        response = requests.get(f"http://schema-registry:8081/subjects/{topic_name}-value/versions/latest")
        return response.json()['id']

    # 🔥 UDF: Thêm Header Confluent (Dùng pystruct thay vì struct)
    def add_header(binary_data):
        if binary_data is None: return None
        # 🔥 [CHANGE] Dùng pystruct.pack
        header = pystruct.pack('>bI', 0, output_schema_id)
        return header + binary_data

    add_header_udf = udf(add_header, BinaryType())

    # READ STREAM
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", INPUT_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # TRANSFORM
    avro_payload = expr("substring(value, 6, length(value)-5)")
    decoded_df = raw_df.select(from_avro(avro_payload, input_avro_schema).alias("data")).select("data.*")

    processed_df = decoded_df \
        .withColumn("item_idx", lookup_udf(col("item_id"))) \
        .withColumn("processed_at", current_timestamp().cast("long"))

    # WRITE STREAM
    print(f"⏳ Đang xử lý stream từ {INPUT_TOPIC} -> {OUTPUT_TOPIC} (AVRO PRO)...")
    print(add_header_udf(col("raw_avro")).alias("value"))
    output_kafka_df = processed_df.select(
        col("user_id").alias("key"),
        to_avro(struct("*"), output_avro_schema).alias("raw_avro")
    ).select(
        col("key"),
        add_header_udf(col("raw_avro")).alias("value")
    )
    query = output_kafka_df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("topic", OUTPUT_TOPIC) \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .outputMode("append") \
        .trigger(processingTime="5 seconds") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()