# import time
# import json
# import random
# import os
# import uuid
# from datetime import datetime

# # Thư viện Confluent Kafka hỗ trợ Avro
# from confluent_kafka import SerializingProducer
# from confluent_kafka.serialization import StringSerializer
# from confluent_kafka.schema_registry import SchemaRegistryClient
# from confluent_kafka.schema_registry.avro import AvroSerializer

# # ==========================================
# # CẤU HÌNH (CONFIG)
# # ==========================================
# KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092')
# SCHEMA_REGISTRY_URL = os.getenv('SCHEMA_REGISTRY_URL', 'http://localhost:9081')
# TOPIC = 'raw_clicks_avro'  # Tên topic cho data Avro

# # Path load dữ liệu giả lập
# BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# PROJECT_ROOT = os.path.dirname(os.path.dirname(BASE_DIR))
# VALID_USERS_PATH = os.path.join(PROJECT_ROOT, 'src/simulation/users.json')
# ITEM_MAP_PATH = os.path.join(PROJECT_ROOT, 'data/model_registry/item_map.json')

# # ==========================================
# # ĐỊNH NGHĨA AVRO SCHEMA
# # ==========================================
# KEY_SCHEMA_STR = """
# {
#   "type": "string"
# }
# """

# VALUE_SCHEMA_STR = """
# {
#   "namespace": "ecommerce.tracking",
#   "type": "record",
#   "name": "UserClick",
#   "fields": [
#     {"name": "event_id", "type": "string"},
#     {"name": "session_id", "type": "string"},
#     {"name": "user_id", "type": "string"},
#     {"name": "item_id", "type": "string"},
#     {"name": "event_type", "type": "string"},
#     {"name": "rating_original", "type": "float"},
#     {"name": "timestamp", "type": "string"},
#     {"name": "context", "type": {
#         "type": "record",
#         "name": "ContextData",
#         "fields": [
#             {"name": "device", "type": "string"},
#             {"name": "location", "type": "string"},
#             {"name": "ip", "type": "string"}
#         ]
#     }}
#   ]
# }
# """

# # ==========================================
# # LOAD DATA HELPER
# # ==========================================
# def load_valid_data():
#     print(f"📂 Đang đọc User từ: {VALID_USERS_PATH}")
    
#     if not os.path.exists(VALID_USERS_PATH):
#         print("❌ LỖI: Không tìm thấy valid_users.json.")
#         exit()

#     with open(VALID_USERS_PATH, 'r') as f:
#         users = json.load(f)

#     with open(ITEM_MAP_PATH, 'r') as f:
#         item_map = json.load(f)
#         # item_map format: {"ItemID_Str": Index_Int} -> Lấy keys là ItemID
#         items = list(item_map.values())

#     print(f"✅ Đã load: {len(users)} Users và {len(items)} Items.")
#     return users, items

# # ==========================================
# # CALLBACK FUNCTION
# # ==========================================
# def delivery_report(err, msg):
#     """Hàm này được gọi khi Kafka xác nhận đã nhận tin nhắn"""
#     if err is not None:
#         print(f"❌ Delivery failed for User record {msg.key()}: {err}")
#     # else:
#     #     print(f"✅ Record {msg.key()} produced to {msg.topic()} [{msg.partition()}]")

# # ==========================================
# # MAIN LOOP
# # ==========================================
# def main():
#     # 1. Load Data
#     valid_users, valid_items = load_valid_data()
    
#     # 2. Setup Schema Registry Client
#     schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
#     schema_registry_client = SchemaRegistryClient(schema_registry_conf)

#     # 3. Setup Avro Serializer
#     avro_serializer = AvroSerializer(
#         schema_registry_client,
#         VALUE_SCHEMA_STR
#     )

#     # 4. Setup Producer Configuration
#     producer_conf = {
#         'bootstrap.servers': KAFKA_BOOTSTRAP,
#         'key.serializer': StringSerializer('utf_8'),
#         'value.serializer': avro_serializer
#     }

#     producer = SerializingProducer(producer_conf)

#     print(f"🚀 Bắt đầu bắn Avro vào topic: {TOPIC}...")

#     try:
#         while True:
#             # --- Tạo dữ liệu giả lập ---
#             user_id = str(random.choice(valid_users))
#             item_id = str(random.choice(valid_items))
#             print(item_id)
            
#             event_type = random.choice(['click', 'view', 'add_to_cart', 'purchase'])
#             device = random.choice(['mobile', 'desktop', 'tablet'])
#             location = random.choice(['Hanoi', 'HCM', 'Danang', 'Cantho'])
            
#             # Tạo Object Python khớp hoàn toàn với Schema Avro
#             value_obj = {
#                 "event_id": str(uuid.uuid4()),
#                 "session_id": str(uuid.uuid4()),
#                 "user_id": user_id,
#                 "item_id": item_id,
#                 "event_type": event_type,
#                 "rating_original": round(random.uniform(1.0, 5.0), 1),
#                 "timestamp": datetime.now().isoformat(),
#                 # Nested Record (Context)
#                 "context": {
#                     "device": device,
#                     "location": location,
#                     "ip": f"192.168.1.{random.randint(1, 255)}"
#                 }
#             }

#             print(f"📤 Sending Avro: User={user_id} -> Item={item_id} | Type={event_type}")

#             # --- Gửi tin nhắn ---
#             # Producer này sẽ tự động:
#             # 1. Đăng ký Schema lên Registry (nếu chưa có).
#             # 2. Lấy Schema ID.
#             # 3. Chèn 5-byte header + data binary.
#             producer.produce(
#                 topic=TOPIC,
#                 key=user_id, # Dùng UserID làm Key để partition đúng
#                 value=value_obj,
#                 on_delivery=delivery_report
#             )

#             # Poll để trigger callback delivery_report
#             producer.poll(0)
            
#             time.sleep(0.002) # Bắn 1 tin/giây

#     except KeyboardInterrupt:
#         print("\n🛑 Dừng simulation.")
#     except Exception as e:
#         print(f"❌ Lỗi Producer: {e}")
#     finally:
#         print("⏳ Đang flush dữ liệu còn sót lại...")
#         producer.flush()

# if __name__ == "__main__":
#     main()

import time
import json
import random
import os
import uuid
import argparse
from datetime import datetime, timedelta

# Thư viện Confluent Kafka
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# ==========================================
# CẤU HÌNH (CONFIG)
# ==========================================
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092')
SCHEMA_REGISTRY_URL = os.getenv('SCHEMA_REGISTRY_URL', 'http://localhost:9081')
TOPIC = 'raw_clicks_avro'

# Path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(BASE_DIR))
VALID_USERS_PATH = os.path.join(PROJECT_ROOT, 'src/simulation/users.json')
ITEM_MAP_PATH = os.path.join(PROJECT_ROOT, 'data/model_registry/item_map.json')

# ==========================================
# SCHEMA (Giữ nguyên)
# ==========================================
VALUE_SCHEMA_STR = """
{
  "namespace": "ecommerce.tracking",
  "type": "record",
  "name": "UserClick",
  "fields": [
    {"name": "event_id", "type": "string"},
    {"name": "session_id", "type": "string"},
    {"name": "user_id", "type": "string"},
    {"name": "item_id", "type": "string"},
    {"name": "event_type", "type": "string"},
    {"name": "rating_original", "type": "float"},
    {"name": "timestamp", "type": "string"},
    {"name": "context", "type": {
        "type": "record",
        "name": "ContextData",
        "fields": [
            {"name": "device", "type": "string"},
            {"name": "location", "type": "string"},
            {"name": "ip", "type": "string"}
        ]
    }}
  ]
}
"""

def load_valid_data():
    if not os.path.exists(VALID_USERS_PATH):
        # Fallback nếu chưa có file user
        print("⚠️ Không tìm thấy file users.json, tạo user giả lập tạm thời.")
        return [f"User_{i}" for i in range(100)], [f"Item_{i}" for i in range(1000)]

    with open(VALID_USERS_PATH, 'r') as f:
        users = json.load(f)
    with open(ITEM_MAP_PATH, 'r') as f:
        item_map = json.load(f)
        # Lấy Key (ASIN/ID gốc) thay vì Value
        items = list(item_map.keys()) 
    return users, items

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Message delivery failed: {err}")

# ==========================================
# MAIN SIMULATION LOGIC
# ==========================================
def main():
    # 1. Cấu hình tham số chạy
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=14, help="Số ngày muốn giả lập (VD: 14 ngày)")
    parser.add_argument("--msgs-per-day", type=int, default=2000, help="Số message mỗi ngày")
    args = parser.parse_args()

    # 2. Setup Kafka
    users, items = load_valid_data()
    schema_registry_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
    avro_serializer = AvroSerializer(schema_registry_client, VALUE_SCHEMA_STR)
    
    producer_conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP,
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': avro_serializer,
        'queue.buffering.max.messages': 50000 # Tăng buffer để bắn nhanh hơn
    }
    producer = SerializingProducer(producer_conf)

    # 3. Tính toán thời gian bắt đầu (Lùi lại N ngày so với hiện tại)
    # Ví dụ: Hôm nay 23/12, lùi 14 ngày -> Bắt đầu từ 09/12
    start_date = datetime.now() - timedelta(days=args.days)
    
    print(f"🚀 BẮT ĐẦU GIẢ LẬP: {args.days} ngày | {args.msgs_per_day} msg/ngày")
    print(f"📅 Thời gian dữ liệu (Event Time): Từ {start_date.strftime('%Y-%m-%d')} đến {datetime.now().strftime('%Y-%m-%d')}")
    print("-" * 50)

    total_sent = 0

    try:
        # --- VÒNG LẶP NGÀY (Từ ngày xưa -> Hôm nay) ---
        for day_offset in range(args.days):
            current_day = start_date + timedelta(days=day_offset)
            print(f"📅 Đang sinh dữ liệu cho ngày: {current_day.strftime('%Y-%m-%d')} ...")

            # --- VÒNG LẶP MESSAGE TRONG NGÀY ---
            for _ in range(args.msgs_per_day):
                # Random thời gian trong vòng 24h của ngày hôm đó
                random_second = random.randint(0, 86399)
                event_time = current_day.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(seconds=random_second)

                user_id = str(random.choice(users))
                item_id = str(random.choice(items))
                event_type = random.choice(['click', 'click', 'click', 'view', 'purchase']) # Ưu tiên click nhiều hơn

                value_obj = {
                    "event_id": str(uuid.uuid4()),
                    "session_id": str(uuid.uuid4()),
                    "user_id": user_id,
                    "item_id": item_id,
                    "event_type": event_type,
                    "rating_original": round(random.uniform(1.0, 5.0), 1),
                    "timestamp": event_time.isoformat(), # <--- Dùng thời gian giả lập
                    "context": {
                        "device": random.choice(['mobile', 'desktop']),
                        "location": random.choice(['Hanoi', 'HCM']),
                        "ip": "127.0.0.1"
                    }
                }

                producer.produce(
                    topic=TOPIC,
                    key=user_id,
                    value=value_obj,
                    on_delivery=delivery_report
                )
                
                total_sent += 1
                
                # Cứ 1000 tin thì poll một lần để giải phóng bộ nhớ (nhanh hơn sleep từng tin)
                if total_sent % 1000 == 0:
                    producer.poll(0)
            
            # Flush nhẹ sau mỗi ngày để đảm bảo data vào Kafka theo thứ tự tương đối
            producer.flush()
            print(f"   ✅ Xong ngày {current_day.strftime('%Y-%m-%d')} ({args.msgs_per_day} msgs)")

    except KeyboardInterrupt:
        print("\n🛑 Dừng simulation.")
    except Exception as e:
        print(f"❌ Lỗi: {e}")
    finally:
        producer.flush()
        print(f"\n🎉 HOÀN TẤT! Tổng cộng {total_sent} bản tin đã được gửi vào Kafka.")

if __name__ == "__main__":
    main()