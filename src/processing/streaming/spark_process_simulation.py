import os
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

# 🔥 OUTPUT RIÊNG CHO DATA MỚI
OUTPUT_PARQUET_NEW = os.path.join(PROJECT_ROOT, 'data/model_registry/processed_parquet_new')

MIN_TS = 1514764800  # 2018-01-01
SIMULATION_DAYS = 30 # Lấy 30 ngày cuối

def main():
    spark = SparkSession.builder \
        .appName("RecSys_Simulation_Data_Gen") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    print(f"🚀 GENERATING SIMULATION DATA (Last {SIMULATION_DAYS} days)...")

    # 1. SCHEMA
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

    # 2. READ DATA
    df_reviews = spark.read.schema(review_schema).json(RAW_DATA)
    # Fix timestamp miliseconds if needed
    if df_reviews.head()['timestamp'] > 32503680000:
        df_reviews = df_reviews.withColumn("timestamp", (F.col("timestamp") / 1000).cast(LongType()))
        
    df_reviews = df_reviews.withColumnRenamed("parent_asin", "item_id") \
        .dropna().filter(F.col("rating") >= 3.0).filter(F.col("timestamp") >= MIN_TS)

    # 3. MERGE METADATA & 🔥 APPLY SAME CATEGORY LOGIC 🔥
    if os.path.exists(META_DATA):
        df_meta = spark.read.schema(meta_schema).json(META_DATA)
        df_meta = df_meta.withColumnRenamed("parent_asin", "item_id")
        df_meta = df_meta.withColumn("t_lower", F.lower(F.col("title")))

        # 👇 ĐÂY LÀ ĐOẠN QUAN TRỌNG: COPY Y NGUYÊN TỪ FILE GỐC SANG 👇
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
            .when(F.col("t_lower").contains("monitor"), "Monitor")
            .when(F.col("t_lower").contains("cable") | F.col("t_lower").contains("charger"), "Power_Cables")
            .when(F.col("t_lower").contains("dlc") | F.col("t_lower").contains("digital code"), "Digital_Content")
            .when(F.col("t_lower").contains("vr") | F.col("t_lower").contains("oculus"), "VR_Gaming")
            .otherwise("Other_Gaming")
        )
        # 👆 KẾT THÚC ĐOẠN LOGIC 👆
        
        df_meta = df_meta.select("item_id", "category_final").dropDuplicates(["item_id"])
        df_joined = df_reviews.join(df_meta, on="item_id", how="left").fillna({"category_final": "Unknown"})
    else:
        df_joined = df_reviews.withColumn("category_final", F.lit("Unknown"))

    # 4. INDEXING (GLOBAL)
    item_indexer = StringIndexer(inputCol="item_id", outputCol="item_idx_raw", handleInvalid="skip").fit(df_joined)
    df_indexed = item_indexer.transform(df_joined)
    cat_indexer = StringIndexer(inputCol="category_final", outputCol="cat_idx_raw", handleInvalid="keep").fit(df_indexed)
    df_indexed = cat_indexer.transform(df_indexed)

    df_indexed = df_indexed.withColumn("item_idx", (F.col("item_idx_raw") + 1).cast(IntegerType())) \
                           .withColumn("category_ids", (F.col("cat_idx_raw") + 1).cast(IntegerType()))

    # 5. SPLIT LAST 30 DAYS
    max_ts = df_indexed.agg(F.max("timestamp")).collect()[0][0]
    cutoff_ts = max_ts - (SIMULATION_DAYS * 24 * 3600)
    
    print(f"✂️ Cutting data from: {datetime.fromtimestamp(cutoff_ts)}")
    df_new = df_indexed.filter(F.col("timestamp") >= cutoff_ts)

    # 6. GROUP & SAVE
    df_grouped = df_new.groupBy("user_id").agg(
        F.sort_array(F.collect_list(F.struct("timestamp", "item_idx", "category_ids"))).alias("events")
    )
    df_final = df_grouped.select(
        F.col("user_id"),
        F.col("events.item_idx").alias("sequence_ids"),
        F.col("events.category_ids").alias("category_ids"),
        F.col("events.timestamp").alias("sequence_timestamps"),
        F.element_at(F.col("events.timestamp"), -1).alias("last_timestamp")
    )

    # Lọc rác: User phải tương tác >= 2 lần trong tháng này mới đáng để học
    df_final = df_final.filter(F.size(F.col("sequence_ids")) >= 2)

    if os.path.exists(OUTPUT_PARQUET_NEW):
        shutil.rmtree(OUTPUT_PARQUET_NEW)
    df_final.write.parquet(OUTPUT_PARQUET_NEW)
    
    print(f"✅ Saved {df_final.count()} users to {OUTPUT_PARQUET_NEW}")
    spark.stop()

if __name__ == "__main__":
    main()