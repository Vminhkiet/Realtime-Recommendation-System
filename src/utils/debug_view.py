import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Config
MINIO_ENDPOINT = "http://minio:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
# Đường dẫn gốc
S3_INPUT = "s3a://datalake/topics/processed_clicks"

def main():
    spark = SparkSession.builder \
        .appName("Debug_View_Raw") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    print("🔍 BẮT ĐẦU DEBUG DỮ LIỆU...")

    try:
        # 1. Đọc dữ liệu
        df = spark.read.option("basePath", S3_INPUT) \
                       .option("mergeSchema", "true") \
                       .option("recursiveFileLookup", "true") \
                       .parquet(S3_INPUT)
        
        # 2. In ra 20 dòng đầu tiên (Raw Data)
        print("\n--- 1. MẪU DỮ LIỆU THÔ (TOP 20) ---")
        # Chỉ chọn các cột quan trọng để hiển thị cho gọn
        df.select("user_id", "item_id", "timestamp", "item_idx").show(20, truncate=False)

        # 3. Kiểm tra User ID
        print("\n--- 2. THỐNG KÊ USER ---")
        user_counts = df.groupBy("user_id").count()
        user_counts.show(10, truncate=False)
        
        total_rows = df.count()
        null_users = df.filter(F.col("user_id").isNull()).count()
        print(f"📊 Tổng số dòng: {total_rows}")
        print(f"❌ Số dòng bị NULL User ID: {null_users}")

        if total_rows > 0 and null_users == total_rows:
            print("🚨 LỖI LỚN: Toàn bộ User ID đều bị Null/Rỗng! Kiểm tra lại nguồn Kafka/Avro.")

    except Exception as e:
        print(f"❌ Lỗi: {e}")

    spark.stop()

if __name__ == "__main__":
    main()