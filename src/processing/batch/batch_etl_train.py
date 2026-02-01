import os
import sys
import json
import boto3
import shutil
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, ArrayType, LongType, StructType, StructField, StringType
from datetime import datetime, timedelta
import json
# ================== 1. CẤU HÌNH (CONFIGURATION) ==================
if len(sys.argv) > 2:
    INCREMENTAL_START_DATE = sys.argv[1]
    INCREMENTAL_END_DATE   = sys.argv[2]
else:
    INCREMENTAL_START_DATE = "2025-12-01"
    INCREMENTAL_END_DATE   = "2025-12-30"

fmt = "%Y-%m-%d"
TEST_END_DATE = (datetime.strptime(INCREMENTAL_END_DATE, fmt) + timedelta(days=7)).strftime(fmt)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET_NAME = "datalake"

# INPUT
S3_NEW_DATA_INPUT = f"s3a://{BUCKET_NAME}/topics/processed_clicks"
S3_BASE_5YEARS = f"s3a://{BUCKET_NAME}/processed_parquet"

# OUTPUT LOCAL & REMOTE
LOCAL_TEMP_DIR = "/home/spark/work/temp_data"
current_date_str = datetime.now().strftime("%Y-%m-%d")

S3_DEST_TRAIN = f"training_data/TRAIN_merged_{current_date_str}"
S3_DEST_TEST  = f"training_data/TEST_merged_{current_date_str}"
S3_ACCUMULATED_PATH = f"s3a://{BUCKET_NAME}/model_registry/incremental_accumulated"

MAX_SEQUENCE_LENGTH = 50

# ================== HELPER FUNCTIONS ==================
def upload_folder_to_s3(local_path, s3_prefix):
    """Upload folder local lên MinIO/S3"""
    print(f"⬆️  Uploading local: {local_path} -> s3://{BUCKET_NAME}/{s3_prefix}")
    s3 = boto3.client('s3', endpoint_url=MINIO_ENDPOINT,
                      aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY, use_ssl=False)
    
    # Xóa dữ liệu cũ trong folder đích (tránh trộn lẫn)
    try:
        objects = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=s3_prefix)
        if 'Contents' in objects:
            for obj in objects['Contents']:
                s3.delete_object(Bucket=BUCKET_NAME, Key=obj['Key'])
    except: pass

    count = 0
    for root, dirs, files in os.walk(local_path):
        for file in files:
            if file.endswith(".parquet") or file.endswith(".json") or file.endswith(".crc"):
                local_file = os.path.join(root, file)
                rel_path = os.path.relpath(local_file, local_path)
                s3_key = f"{s3_prefix}/{rel_path}"
                try:
                    s3.upload_file(local_file, BUCKET_NAME, s3_key)
                    count += 1
                except Exception as e:
                    print(f"   ❌ Upload lỗi {file}: {e}")
    print(f"✅ Đã upload {count} files.")
    return f"s3a://{BUCKET_NAME}/{s3_prefix}"

def update_json_to_minio(new_data_dict, filename):
    """Cập nhật file Config JSON trên MinIO"""
    s3 = boto3.client('s3', endpoint_url=MINIO_ENDPOINT,
                      aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY, use_ssl=False)
    key = f"model_registry/{filename}"
    current_config = {}
    try:
        response = s3.get_object(Bucket=BUCKET_NAME, Key=key)
        current_config = json.loads(response['Body'].read().decode('utf-8'))
    except:
        print("   ⚠️ Config chưa tồn tại, tạo mới.")
    
    current_config.update(new_data_dict)
    
    try:
        s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=json.dumps(current_config, indent=4), ContentType='application/json')
        print(f"✅ [Metadata] Updated: {key}")
    except Exception as e:
        print(f"❌ [Metadata] Error: {e}")

# ================== MAIN JOB (FIXED MAPPING LOGIC) ==================
def main():
    print(f"🚀 ETL MERGE (LEFT JOIN TOPICS) | {INCREMENTAL_START_DATE}")

    if os.path.exists(LOCAL_TEMP_DIR): shutil.rmtree(LOCAL_TEMP_DIR)
    os.makedirs(LOCAL_TEMP_DIR)

    spark = SparkSession.builder \
        .appName("ETL_Fixed_Mapping_Final") \
        .config("spark.driver.memory", "4g") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # ---------------------------------------------------------
    # 0. LOAD MASTER MAP & PREPARE BROADCAST (CHẠY 1 LẦN)
    # ---------------------------------------------------------
    print("0️⃣  Loading Master Map (Fix ID Shift)...")
    try:
        s3_client = boto3.client('s3', endpoint_url=MINIO_ENDPOINT, 
                                 aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
        
        obj = s3_client.get_object(Bucket=BUCKET_NAME, Key="model_registry/item_map.json")
        raw_map = json.loads(obj['Body'].read().decode('utf-8'))
        
        # 🔥 QUAN TRỌNG: Đảo ngược Map
        # JSON gốc:   {"478": "B00..."}  (Key là Index, Value là ID)
        # Cần dùng:   {"B00...": 478}    (Để tra từ ID ra Index)
        item_map_dict = {v: int(k) for k, v in raw_map.items()}
        
        broadcast_map = spark.sparkContext.broadcast(item_map_dict)
        print(f"✅ Loaded & Swapped Map: {len(item_map_dict)} items.")
        
        # Định nghĩa UDF
        def get_master_id(item_str):
            return broadcast_map.value.get(item_str, None)
        lookup_udf = F.udf(get_master_id, IntegerType())
        
    except Exception as e:
        print(f"❌ CRITICAL: Không load được item_map.json: {e}")
        return

    # ---------------------------------------------------------
    # 1. LOAD NEW DATA (TOPICS)
    # ---------------------------------------------------------
    print("1️⃣  Loading New Data (TOPICS)...")
    try:
        df_new_raw = spark.read.parquet(S3_NEW_DATA_INPUT)
        
        # --- Flatten Struct ---
        cols_to_check = ["user_id", "timestamp", "event_type", "item_id"] 
        df_clean = df_new_raw
        
        for col_name in cols_to_check:
            if col_name in df_clean.columns:
                dtype = dict(df_clean.dtypes)[col_name]
                if "struct" in dtype and "string" in dtype:
                    df_clean = df_clean.withColumn(col_name, F.col(f"{col_name}.string"))
        
        # --- Map ID ---
        if "item_id" in df_clean.columns:
            print("   🔄 Remapping item_idx based on Master Map...")
            df_clean = df_clean.withColumn("item_idx", lookup_udf(F.col("item_id")))
            
            # Debug nhanh
            mapped_count = df_clean.filter(F.col("item_idx").isNotNull()).count()
            print(f"   📊 Mapping Stats: {mapped_count} items matched successfully.")
        else:
            print("   ⚠️ CẢNH BÁO: Không tìm thấy cột 'item_id'.")

        # --- Filter Time ---
        print(f"⏱️  ETL WINDOW: [{INCREMENTAL_START_DATE} -> {TEST_END_DATE})")
        df_clean = (
            df_clean
            .withColumn("event_ts", F.to_timestamp("timestamp"))
            .filter(F.col("item_idx").isNotNull()) # Loại bỏ cái nào không map được
            .filter(
                (F.col("event_ts") >= F.lit(INCREMENTAL_START_DATE)) &
                (F.col("event_ts") <  F.lit(TEST_END_DATE))
            )
        )
        
        accepted_events = ["view", "click", "purchase", "add_to_cart"]
        df_new = df_clean.filter(F.col("event_type").isin(accepted_events))
        
        # --- Split Train/Future ---
        df_train_rows = df_new.filter(F.col("event_ts") < F.lit(INCREMENTAL_END_DATE))
        df_future_rows = df_new.filter(F.col("event_ts") >= F.lit(INCREMENTAL_END_DATE))

        def agg_actions(df, alias_name):
            return df.groupBy("user_id").agg(
                F.sort_array(F.collect_list(F.struct(
                    F.col("event_ts").cast("long").alias("ts"),
                    F.col("item_idx").cast("int").alias("item_idx"),
                    F.col("category_ids").cast("int").alias("category_ids")
                ))).alias(alias_name)
            )

        df_new_grouped   = agg_actions(df_train_rows, "new_actions")
        df_future_labels = agg_actions(df_future_rows, "future_actions")

        print(f"   👉 Active Users in Topic (windowed): {df_new_grouped.count()}")

    except Exception as e:
        print(f"⚠️ Lỗi xử lý Topics: {e}")
        return

    # Chuẩn bị DataFrame để Merge
    df_user_items = df_new_grouped.select(
        "user_id",
        F.expr("transform(new_actions, x -> x.item_idx)").alias("item_idxs"),
        F.expr("transform(new_actions, x -> x.category_ids)").alias("category_idxs")
    )

    # ---------------------------------------------------------
    # 2. MERGE WITH HISTORY
    # ---------------------------------------------------------
    print("2️⃣  Merging with History (Left Join)...")
    try:
        df_old = spark.read.parquet(S3_BASE_5YEARS) \
                      .withColumnRenamed("sequence_ids", "old_items") \
                      .withColumnRenamed("category_ids", "old_cats") \
                      .withColumnRenamed("last_timestamp", "old_ts")
    except:
        print("⚠️ Không tìm thấy Base 5 Years.")
        df_old = spark.createDataFrame([], StructType([StructField("user_id", StringType(), True)]))

    df_merged = df_user_items.join(df_old, on="user_id", how="left")
    
    col_old_items = F.coalesce(F.col("old_items").cast("array<int>"), F.array().cast("array<int>"))
    col_old_cats = F.coalesce(F.col("old_cats").cast("array<int>"), F.array().cast("array<int>"))
    col_new_items = F.coalesce(F.col("item_idxs").cast("array<int>"), F.array().cast("array<int>"))
    col_new_cats = F.coalesce(F.col("category_idxs").cast("array<int>"), F.array().cast("array<int>"))

    df_combined = df_merged.select(
        "user_id",
        F.concat(col_old_items, col_new_items).alias("full_items"),
        F.concat(col_old_cats, col_new_cats).alias("full_cats"),
        F.coalesce(F.col("old_ts"), F.lit(0)).alias("last_ts")
    )
    
    # ---------------------------------------------------------
    # 3. SPLIT & SAVE
    # ---------------------------------------------------------
    df_valid = df_combined.filter(F.size("full_items") >= 5)
    
    print("3️⃣  Splitting Train/Test (Future Prediction)...")
    
    df_test = df_valid.join(df_future_labels, on="user_id", how="inner").select(
        "user_id", 
        F.col("full_items").alias("sequence_ids"),
        F.col("full_cats").alias("category_ids"), 
        F.expr("transform(future_actions, x -> x.item_idx)").alias("ground_truth_items"),
        "last_ts"
    )
    
    df_train = df_valid.select(
        "user_id", 
        F.expr("slice(full_items, 1, size(full_items)-1)").alias("sequence_ids"), 
        F.expr("slice(full_cats, 1, size(full_cats)-1)").alias("category_ids"), 
        "last_ts"
    )
    
    print(f"   📊 Train: {df_train.count()} | Test: {df_test.count()}")

    # WRITE
    local_train = os.path.join(LOCAL_TEMP_DIR, "train_data")
    local_test = os.path.join(LOCAL_TEMP_DIR, "test_data")
    local_acc = os.path.join(LOCAL_TEMP_DIR, "accumulated")

    df_train.write.mode("overwrite").parquet(f"file://{local_train}")
    df_test.write.mode("overwrite").parquet(f"file://{local_test}")
    
    df_combined.select(
        F.col("full_items").alias("sequence_ids"), 
        F.col("full_cats").alias("category_ids"), 
        F.col("last_ts").alias("last_timestamp"), 
        "user_id"
    ).write.mode("overwrite").parquet(f"file://{local_acc}")

    # METADATA & UPLOAD
    try:
        max_idx = df_combined.select(F.max(F.element_at("full_items", -1))).collect()[0][0]
    except: max_idx = 0

    s3_train = upload_folder_to_s3(local_train, S3_DEST_TRAIN)
    s3_test = upload_folder_to_s3(local_test, S3_DEST_TEST)
    upload_folder_to_s3(local_acc, "model_registry/incremental_accumulated")

    config = {
        "max_item_idx": int(max_idx) if max_idx else 0,
        "train_path": s3_train,
        "test_path": s3_test,
        "execution_date": current_date_str,
        "data_window": f"{INCREMENTAL_START_DATE}_{INCREMENTAL_END_DATE}",
        "test_type": "future_prediction"
    }
    update_json_to_minio(config, "model_meta_config.json")
    
    shutil.rmtree(LOCAL_TEMP_DIR)
    print("🎉 ETL Complete.")
    spark.stop()

if __name__ == "__main__":
    main()