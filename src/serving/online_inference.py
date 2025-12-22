import json
import requests
import time
import sys
import traceback
import redis # <--- THƯ VIỆN QUAN TRỌNG
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

# ==========================================
# 1. CẤU HÌNH HỆ THỐNG
# ==========================================
KAFKA_BROKER = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:9081"
TF_SERVING_URL = "http://localhost:8501/v1/models/sasrec:predict"
TOPIC_NAME = "processed_clicks"
GROUP_ID = "recommendation_inference_redis_final"
ITEM_MAP_PATH = "./data/model_registry/item_map.json"

# --- CẤU HÌNH REDIS ---
# Nếu chạy code này TRONG DOCKER thì host là "redis"
# Nếu chạy code này TRÊN MÁY THẬT (như bạn đang làm) thì host là "localhost"
REDIS_HOST = "localhost" 
REDIS_PORT = 6379
REDIS_DB = 0

# ==========================================
# 2. KHỞI TẠO KẾT NỐI
# ==========================================
print("⏳ Đang khởi tạo hệ thống...")

# 2.1. Load Item Map
try:
    with open(ITEM_MAP_PATH, 'r') as f:
        item_map = json.load(f)
    reverse_item_map = {int(k): v for k, v in item_map.items()}
    print(f"✅ Đã load Item Map ({len(item_map)} items).")
except FileNotFoundError:
    print(f"⚠️ Không tìm thấy file {ITEM_MAP_PATH}. Kết quả sẽ hiển thị dạng 'Index_XXX'.")
    reverse_item_map = {}

# 2.2. Kết nối Redis
try:
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
    redis_client.ping() # Test kết nối
    print(f"✅ Kết nối Redis thành công tại {REDIS_HOST}:{REDIS_PORT}")
except Exception as e:
    print(f"❌ Lỗi kết nối Redis: {e}")
    # sys.exit(1) # Tùy chọn: Có thể tắt app luôn nếu Redis chết

# 2.3. Kết nối Kafka
schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
try:
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    avro_deserializer = AvroDeserializer(schema_registry_client)
except Exception as e:
    print(f"❌ Lỗi Schema Registry: {e}")
    sys.exit(1)

consumer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': GROUP_ID,
    'auto.offset.reset': 'latest'
}
consumer = Consumer(consumer_conf)
consumer.subscribe([TOPIC_NAME])

print(f"✅ Đang lắng nghe topic: {TOPIC_NAME}")
print("---------------------------------------------------------")

# ==========================================
# 3. HÀM CHUẨN BỊ PAYLOAD
# ==========================================
def prepare_tf_serving_payload(history_list):
    MAX_LEN = 50
    if len(history_list) > MAX_LEN:
        processed_history = history_list[-MAX_LEN:]
    else:
        padding = [0.0] * (MAX_LEN - len(history_list))
        processed_history = history_list + padding

    real_item_ids = [float(x) for x in processed_history]
    dummy_safe_sequence = [1.0] * MAX_LEN 

    payload = {
        "signature_name": "serving_default",
        "inputs": {
            "args_0":   [dummy_safe_sequence], 
            "args_0_1": [real_item_ids],       
            "args_0_2": [real_item_ids]        
        }
    }
    return payload

# ==========================================
# 4. MAIN LOOP (INFERENCE & SAVE TO REDIS)
# ==========================================
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None: continue
        if msg.error():
            print(f"Kafka Error: {msg.error()}")
            continue

        try:
            data = avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
            if data is None: continue
            
            current_item_idx = data.get('item_idx')
            user_id = data.get('user_id', 'Unknown')

            if current_item_idx is None: continue

            # =========================================================
            # 🧠 PHẦN QUAN TRỌNG: QUẢN LÝ LỊCH SỬ (SLIDING WINDOW)
            # =========================================================
            history_key = f"hist:{user_id}"  # Key lưu lịch sử xem
            
            # 1. Thêm item mới vào đuôi danh sách (Right Push)
            redis_client.rpush(history_key, current_item_idx)
            
            # 2. Cắt danh sách (Sliding Window): Chỉ giữ lại 50 item mới nhất
            # LTRIM giữ lại các phần tử trong khoảng index (start, stop)
            # -50 nghĩa là lấy từ cái thứ 50 đếm từ dưới lên
            redis_client.ltrim(history_key, -50, -1)
            redis_client.expire(history_key, 30)
            # 3. Lấy toàn bộ lịch sử ra để đưa vào Model
            # LRANGE trả về list các byte (b'123'), cần ép kiểu về int
            raw_history = redis_client.lrange(history_key, 0, -1)
            history_input = [int(x) for x in raw_history]

            print(f"⚡ User [{user_id}] - Vừa click: {current_item_idx}")
            print(f"📚 Context Lịch sử ({len(history_input)} items): {history_input}")

            # =========================================================

            # --- Gửi Request AI (Giờ input đã là list dài đầy đủ) ---
            payload = prepare_tf_serving_payload(history_input)
            
            start_time = time.time()
            response = requests.post(TF_SERVING_URL, json=payload)
            latency = (time.time() - start_time) * 1000

            if response.status_code == 200:
                result = response.json()
                outputs = result.get('outputs') or result.get('predictions')
                
                if isinstance(outputs, dict):
                    final_result = outputs.get('predictions') or list(outputs.values())[0]
                else:
                    final_result = outputs

                if final_result and len(final_result) > 0:
                    top_indices = final_result[0]
                    
                    top_product_ids = []
                    for idx in top_indices:
                        p_id = reverse_item_map.get(int(idx), f"Index_{int(idx)}")
                        top_product_ids.append(p_id)

                    # Lưu kết quả gợi ý vào Redis (Key rec:...)
                    rec_key = f"rec:{user_id}"
                    redis_client.setex(rec_key, 3600, json.dumps(top_product_ids))

                    print(f"💎 Gợi ý mới nhất: {top_product_ids[:3]}...")
                    print("-" * 50)
            else:
                print(f"❌ Lỗi TF Serving: {response.text}")

        except Exception:
            traceback.print_exc()

except KeyboardInterrupt:
    print("\n🛑 Dừng chương trình.")
finally:
    consumer.close()