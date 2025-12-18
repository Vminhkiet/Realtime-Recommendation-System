import os
import json
import shutil
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType, IntegerType
from pyspark.ml.feature import StringIndexer

# --- CẤU HÌNH ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(BASE_DIR))

# Đảm bảo trỏ đúng file All_Beauty
RAW_DATA = os.path.join(PROJECT_ROOT, 'data/raw_source/All_Beauty.jsonl')
META_DATA = os.path.join(PROJECT_ROOT, 'data/raw_source/meta_All_Beauty.jsonl')

OUTPUT_PARQUET = os.path.join(PROJECT_ROOT, 'data/model_registry/processed_parquet')
MAP_OUTPUT = os.path.join(PROJECT_ROOT, 'data/model_registry/item_map.json')
CAT_MAP_OUTPUT = os.path.join(PROJECT_ROOT, 'data/model_registry/category_map.json') 

def main():
    spark = SparkSession.builder \
        .appName("RecSys_Beauty_Keyword_Mining") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    print("🚀 Đang xử lý All_Beauty: Trích xuất Loại sản phẩm từ Title...")

    # 1. SCHEMA
    review_schema = StructType([
        StructField("rating", FloatType(), True),
        StructField("parent_asin", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("timestamp", LongType(), True)
    ])

    meta_schema = StructType([
        StructField("parent_asin", StringType(), True),
        StructField("title", StringType(), True), # 🔥 Quan trọng nhất
        StructField("main_category", StringType(), True),
        StructField("categories", StringType(), True) 
    ])

    # 2. READ DATA
    if not os.path.exists(RAW_DATA): 
        print(f"❌ Không tìm thấy {RAW_DATA}")
        return
        
    df_reviews = spark.read.schema(review_schema).json(RAW_DATA)
    df_reviews = df_reviews.withColumnRenamed("parent_asin", "item_id") \
                           .dropna(subset=["item_id", "user_id"]) \
                           .filter(F.col("rating") >= 3.0)

    if os.path.exists(META_DATA):
        print(f"📘 Reading Metadata...")
        df_meta = spark.read.schema(meta_schema).json(META_DATA)
        df_meta = df_meta.withColumnRenamed("parent_asin", "item_id")
        
        # --- 🔥 CHIẾN THUẬT "BEAUTY KEYWORDS" 🔥 ---
        df_meta = df_meta.withColumn("t_lower", F.lower(F.col("title")))
        
        df_meta = df_meta.withColumn("category_final", 
            # --- MAKEUP ---
            F.when(F.col("t_lower").contains("lipstick"), F.lit("Lipstick"))
             .when(F.col("t_lower").contains("lip gloss"), F.lit("Lip Gloss"))
             .when(F.col("t_lower").contains("foundation"), F.lit("Foundation"))
             .when(F.col("t_lower").contains("mascara"), F.lit("Mascara"))
             .when(F.col("t_lower").contains("eyeliner"), F.lit("Eyeliner"))
             .when(F.col("t_lower").contains("eyeshadow"), F.lit("Eyeshadow"))
             .when(F.col("t_lower").contains("blush"), F.lit("Blush"))
             .when(F.col("t_lower").contains("concealer"), F.lit("Concealer"))
             .when(F.col("t_lower").contains("powder"), F.lit("Powder"))
             .when(F.col("t_lower").contains("polish"), F.lit("Nail Polish"))
             # --- SKINCARE ---
             .when(F.col("t_lower").contains("moisturizer"), F.lit("Moisturizer"))
             .when(F.col("t_lower").contains("cream"), F.lit("Cream"))
             .when(F.col("t_lower").contains("lotion"), F.lit("Lotion"))
             .when(F.col("t_lower").contains("serum"), F.lit("Serum"))
             .when(F.col("t_lower").contains("mask"), F.lit("Face Mask"))
             .when(F.col("t_lower").contains("cleanser"), F.lit("Cleanser"))
             .when(F.col("t_lower").contains("toner"), F.lit("Toner"))
             .when(F.col("t_lower").contains("sunscreen"), F.lit("Sunscreen"))
             .when(F.col("t_lower").contains("oil"), F.lit("Essential Oil"))
             # --- HAIR ---
             .when(F.col("t_lower").contains("shampoo"), F.lit("Shampoo"))
             .when(F.col("t_lower").contains("conditioner"), F.lit("Conditioner"))
             .when(F.col("t_lower").contains("wig"), F.lit("Wig/Extension"))
             .when(F.col("t_lower").contains("comb"), F.lit("Hair Tool"))
             .when(F.col("t_lower").contains("brush"), F.lit("Hair Tool"))
             .when(F.col("t_lower").contains("dryer"), F.lit("Hair Tool"))
             # --- FRAGRANCE ---
             .when(F.col("t_lower").contains("perfume"), F.lit("Perfume"))
             .when(F.col("t_lower").contains("fragrance"), F.lit("Perfume"))
             .when(F.col("t_lower").contains("cologne"), F.lit("Cologne"))
             # --- TOOLS ---
             .when(F.col("t_lower").contains("mirror"), F.lit("Mirror"))
             .when(F.col("t_lower").contains("razor"), F.lit("Shaving"))
             .when(F.col("t_lower").contains("shaver"), F.lit("Shaving"))
             
             # FALLBACK: Nếu không khớp từ nào -> Lấy từ đầu tiên của Title (Thường là Brand)
             .otherwise(F.split(F.col("title"), " ")[0]) 
        )
        
        # Làm sạch fallback brand (bỏ ký tự lạ)
        df_meta = df_meta.withColumn("category_final", F.regexp_replace(F.col("category_final"), r"[^a-zA-Z0-9]", ""))
        
        # Nếu vẫn null/rỗng -> Unknown
        df_meta = df_meta.withColumn("category_final", 
            F.when((F.col("category_final").isNull()) | (F.col("category_final") == ""), F.lit("Unknown"))
             .otherwise(F.col("category_final"))
        )

        df_meta_clean = df_meta.select("item_id", "category_final").dropDuplicates(["item_id"])
        df_joined = df_reviews.join(df_meta_clean, on="item_id", how="left")
        df_joined = df_joined.fillna({"category_final": "Unknown"})
    else:
        df_joined = df_reviews.withColumn("category_final", F.lit("Unknown"))

    # 4. INDEXING
    print("🔢 Indexing...")
    item_indexer = StringIndexer(inputCol="item_id", outputCol="item_idx_raw", handleInvalid="skip")
    item_indexer_model = item_indexer.fit(df_joined)
    df_indexed = item_indexer_model.transform(df_joined)
    
    # 🔥 Lọc nhiễu: Chỉ giữ Category nào xuất hiện > 20 lần
    cat_counts = df_indexed.groupBy("category_final").count().filter(F.col("count") >= 20)
    df_indexed = df_indexed.join(cat_counts, on="category_final", how="inner")
    
    cat_indexer = StringIndexer(inputCol="category_final", outputCol="cat_idx_raw", handleInvalid="keep")
    cat_indexer_model = cat_indexer.fit(df_indexed)
    df_indexed = cat_indexer_model.transform(df_indexed)

    df_indexed = df_indexed.withColumn("item_idx", (F.col("item_idx_raw") + 1).cast(IntegerType())) \
                           .withColumn("category_ids", (F.col("cat_idx_raw") + 1).cast(IntegerType()))

    # 5. AGGREGATION & FILTER
    print("📦 Grouping...")
    df_grouped = df_indexed.groupBy("user_id").agg(
        F.sort_array(F.collect_list(F.struct("timestamp", "item_idx", "category_ids"))).alias("temp_struct")
    )
    
    df_final = df_grouped.select(
        F.col("user_id"),
        F.col("temp_struct.item_idx").alias("sequence_ids"),
        F.col("temp_struct.category_ids").alias("category_ids")
    )
    
    # All_Beauty data thưa, có thể giảm MIN xuống 3 nếu muốn giữ nhiều user hơn
    df_final = df_final.filter(F.size(F.col("sequence_ids")) >= 5)

    # 6. SAVE
    if os.path.exists(OUTPUT_PARQUET): shutil.rmtree(OUTPUT_PARQUET)
    df_final.write.parquet(OUTPUT_PARQUET)

    item_map = {i+1: label for i, label in enumerate(item_indexer_model.labels)}
    with open(MAP_OUTPUT, 'w') as f: json.dump(item_map, f)

    cat_map = {i+1: label for i, label in enumerate(cat_indexer_model.labels)}
    with open(CAT_MAP_OUTPUT, 'w') as f: json.dump(cat_map, f)

    print(f"✅ DONE! Items: {len(item_map)} | Categories: {len(cat_map)}")
    print(f"👀 Danh sách Categories tìm được: {cat_indexer_model.labels[:30]}")
    spark.stop()

if __name__ == "__main__":
    main()