import os
import json
import shutil
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, FloatType, LongType, IntegerType
)
from pyspark.ml.feature import StringIndexer

# ================== CONFIG ==================
# Đường dẫn gốc
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Nhảy ra 3 cấp thư mục để về root project
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(BASE_DIR)))

# Input Files
RAW_DATA = os.path.join(PROJECT_ROOT, 'data/raw_source/Video_Games.jsonl')
META_DATA = os.path.join(PROJECT_ROOT, 'data/raw_source/meta_Video_Games.jsonl')

# Output Chính (Lịch sử để Train Model)
OUTPUT_PARQUET = os.path.join(PROJECT_ROOT, 'data/model_registry/processed_parquet')

# Output Phụ (Dữ liệu Tương lai giả định - Tháng 12/2025)
OUTPUT_INCREMENTAL = os.path.join(PROJECT_ROOT, 'data/model_registry/incremental_dec_2025')

# Output Maps
MAP_OUTPUT = os.path.join(PROJECT_ROOT, 'data/model_registry/item_map.json')
CAT_MAP_OUTPUT = os.path.join(PROJECT_ROOT, 'data/model_registry/category_map.json')
ITEM_CAT_MAP_OUTPUT = os.path.join(PROJECT_ROOT, 'data/model_registry/item_category.json')

# Mốc thời gian
MIN_TS = 1514764800  # 2018-01-01
# Mốc cắt dữ liệu: 01/12/2025 (Để giả lập dữ liệu mới phát sinh)
DEC_2025_START_TS = 1761955200  # 2025-11-01 

# ================== MAIN ==================
def main():
    spark = SparkSession.builder \
        .appName("RecSys_VideoGames_Split_History_vs_Incremental") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    print("🚀 START: Processing Amazon Video Games dataset...")

    # ========== 1. ĐỊNH NGHĨA SCHEMA ==========
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

    # ========== 2. ĐỌC REVIEWS ==========
    if not os.path.exists(RAW_DATA):
        print(f"❌ Missing file: {RAW_DATA}")
        return

    df_reviews = spark.read.schema(review_schema).json(RAW_DATA)

    # Lọc rác
    df_reviews = (
        df_reviews
        .withColumnRenamed("parent_asin", "item_id")
        .dropna(subset=["item_id", "user_id", "timestamp"])
        .filter(F.col("rating") >= 3.0)      # Chỉ lấy rating tốt
        .filter(F.col("timestamp") >= MIN_TS) # Chỉ lấy từ 2018
    )

    print(f"✅ Reviews loaded: {df_reviews.count():,}")

    # ========== 3. ĐỌC & JOIN METADATA ==========
    if os.path.exists(META_DATA):
        print("📘 Reading metadata & Extracting Categories...")
        df_meta = spark.read.schema(meta_schema).json(META_DATA)
        df_meta = df_meta.withColumnRenamed("parent_asin", "item_id")
        df_meta = df_meta.withColumn("t_lower", F.lower(F.col("title")))

        # Logic gán Category dựa trên Title
        df_meta = df_meta.withColumn(
            "category_final",
            F.when(F.col("t_lower").contains("ps5"), "PS5")
            .when(F.col("t_lower").contains("ps4"), "PS4")
            .when(F.col("t_lower").contains("xbox"), "Xbox")
            .when((F.col("t_lower").contains("nintendo") & F.col("t_lower").contains("switch")), "NintendoSwitch")
            .when(F.col("t_lower").contains("pc") | F.col("t_lower").contains("steam"), "PC_Gaming")
            .when(F.col("t_lower").contains("controller"), "Controller")
            .when(F.col("t_lower").contains("headset") | F.col("t_lower").contains("headphone"), "Audio")
            .when(F.col("t_lower").contains("keyboard"), "Keyboard")
            .when(F.col("t_lower").contains("mouse"), "Mouse")
            .otherwise("Other_Gaming")
        )
        
        # Chỉ lấy item_id và category để join
        df_meta_clean = df_meta.select("item_id", "category_final").dropDuplicates(["item_id"])
        
        df_joined = df_reviews.join(df_meta_clean, on="item_id", how="left")
        df_joined = df_joined.fillna({"category_final": "Unknown"})
    else:
        print("⚠️ Metadata not found. Setting all categories to Unknown.")
        df_joined = df_reviews.withColumn("category_final", F.lit("Unknown"))

    # ========== 4. INDEXING (QUAN TRỌNG: LÀM TRÊN TOÀN BỘ DATA) ==========
    print("🔢 Indexing Items & Categories (Global)...")
    
    # StringIndexer cần quét toàn bộ data để gán ID nhất quán
    item_indexer = StringIndexer(inputCol="item_id", outputCol="item_idx_raw", handleInvalid="skip").fit(df_joined)
    df_indexed = item_indexer.transform(df_joined)

    cat_indexer = StringIndexer(inputCol="category_final", outputCol="cat_idx_raw", handleInvalid="keep").fit(df_indexed)
    df_indexed = cat_indexer.transform(df_indexed)

    # Shift index + 1 (để dành số 0 cho padding sau này)
    df_indexed = (
        df_indexed
        .withColumn("item_idx", (F.col("item_idx_raw") + 1).cast(IntegerType()))
        .withColumn("category_ids", (F.col("cat_idx_raw") + 1).cast(IntegerType()))
    )

    # ==============================================================================
    # BƯỚC 5: TÁCH DATA (SPLIT) - HISTORY (TRAIN) vs INCREMENTAL (SIMULATION)
    # ==============================================================================
    print(f"✂️  Splitting Data at Timestamp: {DEC_2025_START_TS} (Dec 1, 2025)")

    # --- A. TẬP LỊCH SỬ (HISTORY) ---
    # Lấy dữ liệu TRƯỚC tháng 12/2025
    df_history_flat = df_indexed.filter(F.col("timestamp") < DEC_2025_START_TS)
    
    # Group lại thành chuỗi hành vi
    df_history_grouped = df_history_flat.groupBy("user_id").agg(
        F.sort_array(F.collect_list(F.struct("timestamp", "item_idx", "category_ids"))).alias("events")
    )

    df_train_final = df_history_grouped.select(
        F.col("user_id"),
        F.col("events.item_idx").alias("sequence_ids"),
        F.col("events.category_ids").alias("category_ids"),
        F.col("events.timestamp").alias("sequence_timestamps"),
        F.element_at(F.col("events.timestamp"), -1).alias("last_timestamp")
    )

    # Lọc K-Core >= 5 (Chỉ giữ User chất lượng để Train Model)
    df_train_final = df_train_final.filter(F.size(F.col("sequence_ids")) >= 3)
    
    # 🔥 Cache tập Train lại để dùng cho bước Thống kê bên dưới
    df_train_final.cache()
    
    print(f"✅ [History] Valid Train Users (K-Core >= 3): {df_train_final.count():,}")

    # --- B. TẬP INCREMENTAL (SIMULATION) ---
    # Lấy dữ liệu TỪ tháng 12/2025 trở đi
    df_incremental_flat = df_indexed.filter(F.col("timestamp") >= DEC_2025_START_TS)
    
    # Group lại theo User (Giữ nguyên, không lọc K-core, để mô phỏng thực tế có cả user mới)
    df_test_final = df_incremental_flat.groupBy("user_id").agg(
        F.sort_array(F.collect_list(F.struct("timestamp", "item_idx", "category_ids"))).alias("events")
    ).select(
        "user_id",
        F.col("events.item_idx").alias("sequence_ids"),
        F.col("events.category_ids").alias("category_ids"),
        F.col("events.timestamp").alias("sequence_timestamps")
    )
    
    # Cache tập Test
    df_test_final.cache()

    # ==============================================================================
    # BƯỚC 6: LƯU FILES PARQUET
    # ==============================================================================
    
    # 1. Lưu History (Base 5 Years)
    print(f"💾 Saving HISTORY Parquet -> {OUTPUT_PARQUET}")
    if os.path.exists(OUTPUT_PARQUET): shutil.rmtree(OUTPUT_PARQUET)
    df_train_final.write.parquet(OUTPUT_PARQUET)

    # 2. Lưu Incremental (Dec 2025)
    print(f"💾 Saving INCREMENTAL Parquet -> {OUTPUT_INCREMENTAL}")
    if os.path.exists(OUTPUT_INCREMENTAL): shutil.rmtree(OUTPUT_INCREMENTAL)
    
    if not df_test_final.rdd.isEmpty():
        df_test_final.write.parquet(OUTPUT_INCREMENTAL)
        
        # ==============================================================================
        # 📊 [NEW] THỐNG KÊ USER CŨ vs MỚI TRONG TẬP INCREMENTAL
        # ==============================================================================
        print("\n" + "="*50)
        print("📊 PHÂN TÍCH NGƯỜI DÙNG THÁNG 12 (SIMULATION DATA)")
        print("="*50)
        
        total_inc_users = df_test_final.count()

        # Đếm User Cũ: Join với tập Train (History) để xem ai đã từng xuất hiện
        # left_semi: Chỉ lấy những dòng bên trái có khớp với bên phải
        returning_users_count = df_test_final.join(df_train_final, on="user_id", how="left_semi").count()

        # Đếm User Mới
        new_users_count = total_inc_users - returning_users_count

        print(f"👉 Tổng User Active (T12/2025):  {total_inc_users:,}")
        print(f"   ✅ Khách Quen (Returning):    {returning_users_count:,}  (Đã có trong History Train)")
        print(f"   🆕 Khách Mới (New/Cold):      {new_users_count:,}  (Lần đầu thấy hoặc history quá ít)")
        
        if total_inc_users > 0:
            print(f"   📈 Tỷ lệ Retention:           {(returning_users_count/total_inc_users)*100:.2f}%")
        print("="*50 + "\n")

    else:
        print("⚠️ No data found in Dec 2025.")

    # ========== 7. LƯU MAPPING JSON ==========
    print("📝 Saving ID Mappings...")
    
    # Item Map (Index -> ASIN)
    item_map = {int(i + 1): label for i, label in enumerate(item_indexer.labels)}
    with open(MAP_OUTPUT, "w") as f: json.dump(item_map, f)

    # Category Map (Index -> Name)
    cat_map = {int(i + 1): label for i, label in enumerate(cat_indexer.labels)}
    with open(CAT_MAP_OUTPUT, "w") as f: json.dump(cat_map, f)

    # Item -> Category Map (Dùng để tra cứu nhanh khi streaming)
    # Lấy từ df_indexed (distinct item_id) để nhanh hơn
    print("🗺️  Generating Item-Category Map...")
    df_map = df_indexed.select("item_idx", "category_ids").distinct()
    rows = df_map.collect()
    item_cat_map = {str(row["item_idx"]): int(row["category_ids"]) for row in rows}
    
    with open(ITEM_CAT_MAP_OUTPUT, "w") as f: json.dump(item_cat_map, f)

    print(f"✅ Maps saved. Total mapped items: {len(item_cat_map)}")
    print("🎉 DONE – ALL DATASETS READY FOR PIPELINE.")

    # Uncache
    df_train_final.unpersist()
    df_test_final.unpersist()
    spark.stop()

if __name__ == "__main__":
    main()