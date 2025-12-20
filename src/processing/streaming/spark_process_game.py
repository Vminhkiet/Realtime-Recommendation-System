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
from datetime import datetime

# ================== CONFIG ==================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(BASE_DIR)))

RAW_DATA = os.path.join(PROJECT_ROOT, 'data/raw_source/Video_Games.jsonl')
META_DATA = os.path.join(PROJECT_ROOT, 'data/raw_source/meta_Video_Games.jsonl')

OUTPUT_PARQUET = os.path.join(PROJECT_ROOT, 'data/model_registry/processed_parquet')
MAP_OUTPUT = os.path.join(PROJECT_ROOT, 'data/model_registry/item_map.json')
CAT_MAP_OUTPUT = os.path.join(PROJECT_ROOT, 'data/model_registry/category_map.json')
# 🔥 [NEW] File map Item->Category dùng cho Inference
ITEM_CAT_MAP_OUTPUT = os.path.join(PROJECT_ROOT, 'data/model_registry/item_category.json')

MIN_TS = 1514764800  # 2018-01-01

# ================== MAIN ==================
def main():
    spark = SparkSession.builder \
        .appName("RecSys_VideoGames_TimeAware") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    print("🚀 Processing Amazon Video Games dataset (TIME-AWARE)...")

    # ========== 1. SCHEMA ==========
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

    # ========== 2. READ REVIEWS ==========
    if not os.path.exists(RAW_DATA):
        print(f"❌ Missing file: {RAW_DATA}")
        return

    df_reviews = spark.read.schema(review_schema).json(RAW_DATA)

    df_reviews = (
        df_reviews
        .withColumnRenamed("parent_asin", "item_id")
        .dropna(subset=["item_id", "user_id", "timestamp"])
        .filter(F.col("rating") >= 3.0)
        .filter(F.col("timestamp") >= MIN_TS)
    )

    print(f"✅ Reviews loaded: {df_reviews.count():,}")

    # ========== 3. READ METADATA ==========
    if os.path.exists(META_DATA):
        print("📘 Reading metadata...")
        df_meta = spark.read.schema(meta_schema).json(META_DATA)
        df_meta = df_meta.withColumnRenamed("parent_asin", "item_id")

        df_meta = df_meta.withColumn("t_lower", F.lower(F.col("title")))

        # Heuristic Mapping
        df_meta = df_meta.withColumn(
            "category_final",
            F.when(F.col("t_lower").contains("ps5"), "PS5")
            .when(F.col("t_lower").contains("ps4"), "PS4")
            .when(F.col("t_lower").contains("xbox"), "Xbox")
            .when(
                (F.col("t_lower").contains("nintendo") & F.col("t_lower").contains("switch")),
                "NintendoSwitch"
            )
            .when(F.col("t_lower").contains("pc") | F.col("t_lower").contains("steam"), "PC_Gaming")
            .when(F.col("t_lower").contains("controller"), "Controller")
            .when(F.col("t_lower").contains("headset") | F.col("t_lower").contains("headphone"), "Audio")
            .when(F.col("t_lower").contains("keyboard"), "Keyboard")
            .when(F.col("t_lower").contains("mouse"), "Mouse")
            .when(F.col("t_lower").contains("monitor"), "Monitor")
            .when(F.col("t_lower").contains("cable") | F.col("t_lower").contains("charger"), "Power_Cables")
            .when(F.col("t_lower").contains("dlc") | F.col("t_lower").contains("digital code"), "Digital_Content")
            .when(F.col("t_lower").contains("vr") | F.col("t_lower").contains("oculus"), "VR_Gaming")
            .otherwise("Other_Gaming")
        )

        df_meta = df_meta.select("item_id", "category_final").dropDuplicates(["item_id"])
        df_joined = df_reviews.join(df_meta, on="item_id", how="left")
        df_joined = df_joined.fillna({"category_final": "Unknown"})
    else:
        print("⚠️ Metadata not found. Using Unknown category.")
        df_joined = df_reviews.withColumn("category_final", F.lit("Unknown"))

    # ========== 4. INDEXING ==========
    print("🔢 Indexing items & categories...")

    item_indexer = StringIndexer(
        inputCol="item_id",
        outputCol="item_idx_raw",
        handleInvalid="skip"
    ).fit(df_joined)

    df_indexed = item_indexer.transform(df_joined)

    cat_indexer = StringIndexer(
        inputCol="category_final",
        outputCol="cat_idx_raw",
        handleInvalid="keep"
    ).fit(df_indexed)

    df_indexed = cat_indexer.transform(df_indexed)

    # +1 cho Index để dành số 0 cho Padding
    df_indexed = (
        df_indexed
        .withColumn("item_idx", (F.col("item_idx_raw") + 1).cast(IntegerType()))
        .withColumn("category_ids", (F.col("cat_idx_raw") + 1).cast(IntegerType()))
    )

    # ========== 5. SEQUENCE AGGREGATION (TIME-AWARE) ==========
    print("📦 Building user sequences (keep timestamps)...")

    df_grouped = df_indexed.groupBy("user_id").agg(
        F.sort_array(
            F.collect_list(
                F.struct(
                    "timestamp",
                    "item_idx",
                    "category_ids"
                )
            )
        ).alias("events")
    )

    df_final = df_grouped.select(
        F.col("user_id"),
        F.col("events.item_idx").alias("sequence_ids"),
        F.col("events.category_ids").alias("category_ids"),
        F.col("events.timestamp").alias("sequence_timestamps"),
        F.element_at(F.col("events.timestamp"), -1).alias("last_timestamp")
    )

    df_final = df_final.filter(F.size(F.col("sequence_ids")) >= 5)

    print(f"✅ Users after filtering: {df_final.count():,}")

    # ========== 6. SAVE PARQUET ==========
    print(f"💾 Saving Parquet → {OUTPUT_PARQUET}")
    if os.path.exists(OUTPUT_PARQUET):
        shutil.rmtree(OUTPUT_PARQUET)

    df_final.write.parquet(OUTPUT_PARQUET)

    # ========== 7. SAVE MAPS ==========
    item_map = {int(i + 1): label for i, label in enumerate(item_indexer.labels)}
    with open(MAP_OUTPUT, "w") as f:
        json.dump(item_map, f)

    cat_map = {int(i + 1): label for i, label in enumerate(cat_indexer.labels)}
    with open(CAT_MAP_OUTPUT, "w") as f:
        json.dump(cat_map, f)

    # ========== 8. LOG TIME RANGE ==========
    stats = df_reviews.select(
        F.min("timestamp").alias("min_ts"),
        F.max("timestamp").alias("max_ts")
    ).collect()[0]

    def normalize(ts):
        return ts / 1000 if ts > 32503680000 else ts

    start_date = datetime.fromtimestamp(normalize(stats["min_ts"]))
    end_date = datetime.fromtimestamp(normalize(stats["max_ts"]))

    print(f"📅 Data range: {start_date} → {end_date}")

    # ========== 9. SAVE ITEM-CATEGORY MAPPING (FOR INFERENCE) ==========
    # 🔥 [NEW SECTION] Tạo file map Item ID (Int) -> Category ID (Int)
    print("🗺️ Generating Item-Category Map for Real-time Inference...")
    
    # Tách cặp (item, category) duy nhất từ dữ liệu đã xử lý
    df_map = df_final.select(
        F.explode(F.arrays_zip("sequence_ids", "category_ids")).alias("pair")
    ).select(
        F.col("pair.sequence_ids").alias("item_id"),
        F.col("pair.category_ids").alias("cat_id")
    ).distinct()

    # Thu thập về Driver và lưu JSON
    rows = df_map.collect()
    # Key là string (để JSON hiểu), Value là int
    item_cat_map = {str(row["item_id"]): int(row["cat_id"]) for row in rows}

    with open(ITEM_CAT_MAP_OUTPUT, "w") as f:
        json.dump(item_cat_map, f)
    
    print(f"✅ Saved Item-Category Map: {len(item_cat_map)} items")

    print("🎉 DONE – TIME-AWARE DATASET READY")

    spark.stop()


if __name__ == "__main__":
    main()