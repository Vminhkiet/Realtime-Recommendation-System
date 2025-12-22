import os
import sys
import json
import boto3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from datetime import datetime, timedelta

# ================== CẤU HÌNH (CONFIGURATION) ==================
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET_NAME = "datalake"

# Đường dẫn Input/Output
S3_ROOT_INPUT = f"s3a://{BUCKET_NAME}/topics/processed_clicks"
current_week = datetime.now().strftime("%Y_week_%U")
S3_OUTPUT_TRAIN = f"s3a://{BUCKET_NAME}/training_data/{current_week}"
S3_MODEL_REGISTRY = "model_registry"

# 🔥 CẤU HÌNH LỌC NGƯỜI DÙNG (QUALITY FILTER) 🔥
# Chỉ những user có ít nhất bao nhiêu hành động mới được đưa vào Train?
MIN_INTERACTIONS = 5 
MAX_SEQUENCE_LENGTH = 50

# ================== HÀM HỖ TRỢ (HELPER) ==================
def upload_json_to_minio(data_dict, filename):
    """Upload file cấu hình JSON lên MinIO (Model Registry)"""
    s3 = boto3.client('s3', endpoint_url=MINIO_ENDPOINT,
                      aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY, 
                      use_ssl=False)
    try:
        key = f"{S3_MODEL_REGISTRY}/{filename}"
        s3.put_object(Bucket=BUCKET_NAME, Key=key,
                      Body=json.dumps(data_dict), ContentType='application/json')
        print(f"✅ Đã upload Config: {key}")
    except Exception as e:
        print(f"❌ Lỗi upload {filename}: {e}")

# ================== MAIN PIPELINE ==================
def main():
    # 1. KHỞI TẠO SPARK
    spark = SparkSession.builder \
        .appName(f"Batch_ETL_{current_week}_PROD") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    print(f"🚀 Bắt đầu Job ETL (Quality Filter >= {MIN_INTERACTIONS}): {current_week}")

    # 2. ĐỌC DỮ LIỆU
    print(f"📂 Đang quét dữ liệu từ: {S3_ROOT_INPUT}")
    try:
        df_raw = spark.read \
            .option("basePath", S3_ROOT_INPUT) \
            .option("mergeSchema", "true") \
            .option("recursiveFileLookup", "true") \
            .parquet(S3_ROOT_INPUT)
        print(f"👉 Tổng số file Parquet tìm thấy: {len(df_raw.inputFiles())}")
    except Exception as e:
        print(f"❌ Lỗi đọc dữ liệu: {e}")
        spark.stop()
        return

    # 3. LÀM SẠCH & CHUẨN HÓA
    if "category_ids" not in df_raw.columns:
        df_raw = df_raw.withColumn("category_ids", F.lit(0).cast(IntegerType()))
    else:
        # Đảm bảo tên cột nhất quán
        if "category_id" in df_raw.columns:
            df_raw = df_raw.withColumnRenamed("category_id", "category_ids")

    # Lọc thời gian (30 ngày gần nhất)
    print("🧹 Đang lọc rác và thời gian...")
    df_clean = df_raw.withColumn("ts_obj", F.to_timestamp(F.col("timestamp")))
    df_filtered = df_clean.filter(
        (F.col("ts_obj").isNotNull()) & 
        (F.col("ts_obj") >= F.date_sub(F.current_timestamp(), 30))
    )

    count = df_filtered.count()
    print(f"✅ Số dòng bản ghi hợp lệ: {count}")
    
    if count == 0:
        print("⚠️ Không có dữ liệu.")
        spark.stop()
        return

    # 4. GOM NHÓM (GROUP BY USER)
    print("🔄 Đang gom nhóm hành vi theo User...")
    df_grouped = df_filtered.withColumn("ts_long", F.col("ts_obj").cast("long")).groupBy("user_id").agg(
        F.sort_array(
            F.collect_list(
                F.struct(
                    F.col("ts_long").alias("ts"), 
                    F.col("item_idx").alias("item"), 
                    F.col("category_ids").alias("cat")
                )
            )
        ).alias("events")
    )

    # --- DEBUG: HIỂN THỊ THỐNG KÊ TRƯỚC KHI LỌC ---
    print("\n📊 --- THỐNG KÊ DỮ LIỆU GỐC (Trước khi cắt/lọc) ---")
    df_grouped.select(
        F.col("user_id"), 
        F.size(F.col("events")).alias("total_interactions")
    ).orderBy(F.col("total_interactions").desc()).show(5, truncate=False)
    # ------------------------------------------------

    # 5. TẠO DATASET & LỌC CHẤT LƯỢNG (QUALITY FILTER)
    df_final = df_grouped.select(
        F.col("user_id"),
        F.slice(F.col("events.item"), -MAX_SEQUENCE_LENGTH, MAX_SEQUENCE_LENGTH).alias("sequence_ids"),
        F.slice(F.col("events.cat"), -MAX_SEQUENCE_LENGTH, MAX_SEQUENCE_LENGTH).alias("category_ids"),
        F.element_at(F.col("events.ts"), -1).alias("last_timestamp")
    )

    # 🔥 QUAN TRỌNG: Chỉ giữ lại User có số lượng tương tác >= MIN_INTERACTIONS
    df_final = df_final.filter(F.size(F.col("sequence_ids")) >= MIN_INTERACTIONS)

    final_count = df_final.count()
    print(f"📉 Kết quả sau khi lọc (User >= {MIN_INTERACTIONS} items): {final_count} User Sequences.")

    if final_count > 0:
        # 6. LƯU FILE PARQUET
        print(f"💾 Đang ghi dữ liệu vào: {S3_OUTPUT_TRAIN}")
        df_final.coalesce(1).write.mode("overwrite").parquet(S3_OUTPUT_TRAIN)

        # 7. METADATA
        print("🗺️ Đang tạo Metadata Config...")
        max_item = df_filtered.agg(F.max("item_idx")).collect()[0][0]
        max_cat = df_filtered.agg(F.max("category_ids")).collect()[0][0]
        
        df_maps = df_filtered.select("item_idx", "category_ids").distinct()
        item_cat_map = {str(r["item_idx"]): int(r["category_ids"]) for r in df_maps.collect()}

        meta_config = {
            "max_item_idx": int(max_item) if max_item else 0,
            "max_cat_idx": int(max_cat) if max_cat else 0,
            "train_path": S3_OUTPUT_TRAIN,
            "updated_at": datetime.now().isoformat(),
            "min_interactions": MIN_INTERACTIONS
        }

        upload_json_to_minio(item_cat_map, "item_category.json")
        upload_json_to_minio(meta_config, "model_meta_config.json")
        
        print("🎉 Batch ETL Hoàn tất thành công!")
    else:
        print(f"⚠️ CẢNH BÁO: Không có User nào có đủ {MIN_INTERACTIONS} hành động. Hãy chạy thêm 'make sim'!")

    spark.stop()

if __name__ == "__main__":
    main()