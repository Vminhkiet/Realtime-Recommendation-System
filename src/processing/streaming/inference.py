from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, LongType
import json
import time
# Import file utils vừa tạo
import utils 
# Import Kafka Producer để bắn kết quả ra
from kafka import KafkaProducer

# Cấu hình Kafka
KAFKA_SERVER = "kafka:29092"
TOPIC_INPUT = "user_clicks"
TOPIC_OUTPUT = "recommendations"

def process_batch(df, batch_id):
    """
    Hàm này chạy mỗi khi có một lô dữ liệu mới từ Kafka (Micro-batch)
    """
    # Nếu batch rỗng thì bỏ qua
    if df.count() == 0: return
    
    # Chuyển Spark DataFrame thành List Python để dễ xử lý logic phức tạp
    rows = df.collect()
    print(f"\n⚡ [Batch {batch_id}] Đang xử lý {len(rows)} clicks...")
    
    # Tạo kết nối Kafka Producer (để gửi kết quả gợi ý)
    producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER, 
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    
    for row in rows:
        user = row['user_id']
        item = row['item_id']
        
        # --- GỌI TRÍ TUỆ NHÂN TẠO ---
        # 1. Update Redis & 2. Predict
        recommendations = utils.AIInferenceService.predict(user, item)
        
        if recommendations:
            # Đóng gói kết quả
            result = {
                "user_id": user,
                "trigger_item": item,
                "recommendations": recommendations, # List các món gợi ý
                "timestamp": int(time.time())
            }
            
            # 3. Bắn ra Kafka (Topic: recommendations)
            producer.send(TOPIC_OUTPUT, result)
            
            # In ra màn hình để Demo cho thầy xem
            print(f"✅ Gợi ý cho {user[:5]}...: {recommendations[:2]}...")
        else:
            print(f"⚠️ Không thể gợi ý cho {user} (Lỗi hoặc Cold Start)")
            
    producer.flush()

def main():
    # Khởi tạo Spark
    spark = SparkSession.builder \
        .appName("SpeedLayer_SASRec") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    # Định nghĩa cấu trúc dữ liệu từ Producer gửi sang
    schema = StructType() \
        .add("user_id", StringType()) \
        .add("item_id", StringType()) \
        .add("timestamp", LongType())

    # Đọc luồng từ Kafka
    df_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVER) \
        .option("subscribe", TOPIC_INPUT) \
        .load()
    
    # Parse JSON từ Kafka
    df_parsed = df_stream.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    # Lọc bỏ dữ liệu lỗi (null user)
    df_clean = df_parsed.filter(col("user_id").isNotNull())

    # Chạy Streaming với hàm process_batch
    query = df_clean.writeStream \
        .foreachBatch(process_batch) \
        .trigger(processingTime='2 seconds') \
        .start()

    print("🚀 Speed Layer đang chạy... Đang chờ dữ liệu từ Kafka...")
    query.awaitTermination()

if __name__ == "__main__":
    main()