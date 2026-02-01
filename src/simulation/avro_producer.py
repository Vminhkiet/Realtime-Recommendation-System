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

# import time
# import json
# import random
# import os
# import uuid
# import argparse
# from datetime import datetime, timedelta

# # Thư viện Confluent Kafka
# from confluent_kafka import SerializingProducer
# from confluent_kafka.serialization import StringSerializer
# from confluent_kafka.schema_registry import SchemaRegistryClient
# from confluent_kafka.schema_registry.avro import AvroSerializer

# # ==========================================
# # CẤU HÌNH (CONFIG)
# # ==========================================
# KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092')
# SCHEMA_REGISTRY_URL = os.getenv('SCHEMA_REGISTRY_URL', 'http://localhost:9081')
# TOPIC = 'raw_clicks_avro'

# # Path
# BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# PROJECT_ROOT = os.path.dirname(os.path.dirname(BASE_DIR))
# VALID_USERS_PATH = os.path.join(PROJECT_ROOT, 'src/simulation/users.json')
# ITEM_MAP_PATH = os.path.join(PROJECT_ROOT, 'data/model_registry/item_map.json')

# # ==========================================
# # SCHEMA (Giữ nguyên)
# # ==========================================
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

# def load_valid_data():
#     if not os.path.exists(VALID_USERS_PATH):
#         # Fallback nếu chưa có file user
#         print("⚠️ Không tìm thấy file users.json, tạo user giả lập tạm thời.")
#         return [f"User_{i}" for i in range(100)], [f"Item_{i}" for i in range(1000)]

#     with open(VALID_USERS_PATH, 'r') as f:
#         users = json.load(f)
#     with open(ITEM_MAP_PATH, 'r') as f:
#         item_map = json.load(f)
#         # Lấy Key (ASIN/ID gốc) thay vì Value
#         items = list(item_map.keys()) 
#     return users, items

# def delivery_report(err, msg):
#     if err is not None:
#         print(f"❌ Message delivery failed: {err}")

# # ==========================================
# # MAIN SIMULATION LOGIC
# # ==========================================
# def main():
#     # 1. Cấu hình tham số chạy
#     parser = argparse.ArgumentParser()
#     parser.add_argument("--days", type=int, default=14, help="Số ngày muốn giả lập (VD: 14 ngày)")
#     parser.add_argument("--msgs-per-day", type=int, default=2000, help="Số message mỗi ngày")
#     args = parser.parse_args()

#     # 2. Setup Kafka
#     users, items = load_valid_data()
#     schema_registry_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
#     avro_serializer = AvroSerializer(schema_registry_client, VALUE_SCHEMA_STR)
    
#     producer_conf = {
#         'bootstrap.servers': KAFKA_BOOTSTRAP,
#         'key.serializer': StringSerializer('utf_8'),
#         'value.serializer': avro_serializer,
#         'queue.buffering.max.messages': 50000 # Tăng buffer để bắn nhanh hơn
#     }
#     producer = SerializingProducer(producer_conf)

#     # 3. Tính toán thời gian bắt đầu (Lùi lại N ngày so với hiện tại)
#     # Ví dụ: Hôm nay 23/12, lùi 14 ngày -> Bắt đầu từ 09/12
#     start_date = datetime.now() - timedelta(days=args.days)
    
#     print(f"🚀 BẮT ĐẦU GIẢ LẬP: {args.days} ngày | {args.msgs_per_day} msg/ngày")
#     print(f"📅 Thời gian dữ liệu (Event Time): Từ {start_date.strftime('%Y-%m-%d')} đến {datetime.now().strftime('%Y-%m-%d')}")
#     print("-" * 50)

#     total_sent = 0

#     try:
#         # --- VÒNG LẶP NGÀY (Từ ngày xưa -> Hôm nay) ---
#         for day_offset in range(args.days):
#             current_day = start_date + timedelta(days=day_offset)
#             print(f"📅 Đang sinh dữ liệu cho ngày: {current_day.strftime('%Y-%m-%d')} ...")

#             # --- VÒNG LẶP MESSAGE TRONG NGÀY ---
#             for _ in range(args.msgs_per_day):
#                 # Random thời gian trong vòng 24h của ngày hôm đó
#                 random_second = random.randint(0, 86399)
#                 event_time = current_day.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(seconds=random_second)

#                 user_id = str(random.choice(users))
#                 item_id = str(random.choice(items))
#                 event_type = random.choice(['click', 'click', 'click', 'view', 'purchase']) # Ưu tiên click nhiều hơn

#                 value_obj = {
#                     "event_id": str(uuid.uuid4()),
#                     "session_id": str(uuid.uuid4()),
#                     "user_id": user_id,
#                     "item_id": item_id,
#                     "event_type": event_type,
#                     "rating_original": round(random.uniform(1.0, 5.0), 1),
#                     "timestamp": event_time.isoformat(), # <--- Dùng thời gian giả lập
#                     "context": {
#                         "device": random.choice(['mobile', 'desktop']),
#                         "location": random.choice(['Hanoi', 'HCM']),
#                         "ip": "127.0.0.1"
#                     }
#                 }

#                 producer.produce(
#                     topic=TOPIC,
#                     key=user_id,
#                     value=value_obj,
#                     on_delivery=delivery_report
#                 )
                
#                 total_sent += 1
                
#                 # Cứ 1000 tin thì poll một lần để giải phóng bộ nhớ (nhanh hơn sleep từng tin)
#                 if total_sent % 1000 == 0:
#                     producer.poll(0)
            
#             # Flush nhẹ sau mỗi ngày để đảm bảo data vào Kafka theo thứ tự tương đối
#             producer.flush()
#             print(f"   ✅ Xong ngày {current_day.strftime('%Y-%m-%d')} ({args.msgs_per_day} msgs)")

#     except KeyboardInterrupt:
#         print("\n🛑 Dừng simulation.")
#     except Exception as e:
#         print(f"❌ Lỗi: {e}")
#     finally:
#         producer.flush()
#         print(f"\n🎉 HOÀN TẤT! Tổng cộng {total_sent} bản tin đã được gửi vào Kafka.")

# if __name__ == "__main__":
#     main()

# import time
# import json
# import random
# import os
# import uuid
# import argparse
# from datetime import datetime, timedelta

# # Thư viện Confluent Kafka
# from confluent_kafka import SerializingProducer
# from confluent_kafka.serialization import StringSerializer
# from confluent_kafka.schema_registry import SchemaRegistryClient
# from confluent_kafka.schema_registry.avro import AvroSerializer

# # ==========================================
# # CẤU HÌNH (CONFIG)
# # ==========================================
# KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092')
# SCHEMA_REGISTRY_URL = os.getenv('SCHEMA_REGISTRY_URL', 'http://localhost:9081')
# TOPIC = 'raw_clicks_avro'

# # Path
# BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# PROJECT_ROOT = os.path.dirname(os.path.dirname(BASE_DIR))
# VALID_USERS_PATH = os.path.join(PROJECT_ROOT, 'src/simulation/users.json')
# ITEM_MAP_PATH = os.path.join(PROJECT_ROOT, 'data/model_registry/item_map.json')

# # ==========================================
# # SCHEMA
# # ==========================================
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

# def load_valid_data():
#     if not os.path.exists(VALID_USERS_PATH):
#         print("⚠️ Không tìm thấy file users.json, tạo user giả lập tạm thời.")
#         return [f"User_{i}" for i in range(100)], [f"Item_{i}" for i in range(1000)]

#     with open(VALID_USERS_PATH, 'r') as f:
#         users = json.load(f)
#     with open(ITEM_MAP_PATH, 'r') as f:
#         item_map = json.load(f)
#         items = list(item_map.keys()) 
#     return users, items

# def delivery_report(err, msg):
#     if err is not None:
#         print(f"❌ Message delivery failed: {err}")

# # ==========================================
# # MAIN SIMULATION LOGIC
# # ==========================================
# def main():
#     # 1. Cấu hình tham số chạy
#     parser = argparse.ArgumentParser()
#     # 🔥 SỬA: Mặc định chạy 14 ngày (2 tuần)
#     parser.add_argument("--days", type=int, default=14, help="Số ngày muốn giả lập")
#     parser.add_argument("--msgs-per-day", type=int, default=2000, help="Số message mỗi ngày")
#     args = parser.parse_args()

#     # 2. Setup Kafka
#     users, items = load_valid_data()
#     schema_registry_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
#     avro_serializer = AvroSerializer(schema_registry_client, VALUE_SCHEMA_STR)
    
#     producer_conf = {
#         'bootstrap.servers': KAFKA_BOOTSTRAP,
#         'key.serializer': StringSerializer('utf_8'),
#         'value.serializer': avro_serializer,
#         'queue.buffering.max.messages': 50000 
#     }
#     producer = SerializingProducer(producer_conf)

#     # 3. Tính toán thời gian bắt đầu
#     # 🔥 SỬA: Bắt đầu từ ngày 23/12/2025
#     # (Khớp với thời điểm kết thúc của tập Train Data đã hack time)
#     start_date = datetime(2025, 12, 23)
    
#     end_date = start_date + timedelta(days=args.days)
    
#     print(f"🚀 BẮT ĐẦU GIẢ LẬP TƯƠNG LAI: {args.days} ngày (2 tuần)")
#     print(f"📅 Thời gian dữ liệu (Event Time): Từ {start_date.strftime('%Y-%m-%d')} đến {end_date.strftime('%Y-%m-%d')}")
#     print("-" * 50)

#     total_sent = 0

#     try:
#         # --- VÒNG LẶP NGÀY (Từ 23/12 -> Tương lai) ---
#         for day_offset in range(args.days):
#             current_day = start_date + timedelta(days=day_offset)
#             print(f"📅 Đang sinh dữ liệu cho ngày: {current_day.strftime('%Y-%m-%d')} ...")

#             # --- VÒNG LẶP MESSAGE TRONG NGÀY ---
#             for _ in range(args.msgs_per_day):
#                 # Random thời gian trong vòng 24h của ngày hôm đó
#                 random_second = random.randint(0, 86399)
#                 event_time = current_day.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(seconds=random_second)

#                 user_id = str(random.choice(users))
#                 item_id = str(random.choice(items))
#                 event_type = random.choice(['click', 'click', 'click', 'view', 'purchase']) 

#                 value_obj = {
#                     "event_id": str(uuid.uuid4()),
#                     "session_id": str(uuid.uuid4()),
#                     "user_id": user_id,
#                     "item_id": item_id,
#                     "event_type": event_type,
#                     "rating_original": round(random.uniform(1.0, 5.0), 1),
#                     "timestamp": event_time.isoformat(), # Dùng thời gian tương lai này
#                     "context": {
#                         "device": random.choice(['mobile', 'desktop']),
#                         "location": random.choice(['Hanoi', 'HCM']),
#                         "ip": "127.0.0.1"
#                     }
#                 }

#                 producer.produce(
#                     topic=TOPIC,
#                     key=user_id,
#                     value=value_obj,
#                     on_delivery=delivery_report
#                 )
                
#                 total_sent += 1
                
#                 if total_sent % 1000 == 0:
#                     producer.poll(0)
            
#             # Flush nhẹ sau mỗi ngày
#             producer.flush()
#             print(f"   ✅ Xong ngày {current_day.strftime('%Y-%m-%d')} ({args.msgs_per_day} msgs)")

#     except KeyboardInterrupt:
#         print("\n🛑 Dừng simulation.")
#     except Exception as e:
#         print(f"❌ Lỗi: {e}")
#     finally:
#         producer.flush()
#         print(f"\n🎉 HOÀN TẤT! Tổng cộng {total_sent} bản tin đã được gửi vào Kafka.")

# if __name__ == "__main__":
#     main()

import time
import json
import os
import uuid
import pandas as pd
import numpy as np
from datetime import datetime

# Thư viện Kafka Client
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# ==========================================
# 1. CẤU HÌNH (CONFIGURATION)
# ==========================================
# Kết nối Kafka & Schema Registry
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092')
SCHEMA_REGISTRY_URL = os.getenv('SCHEMA_REGISTRY_URL', 'http://localhost:9081')
TOPIC = 'raw_clicks_avro'

# Đường dẫn file (Tự động tính toán tương đối)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(BASE_DIR))

# Input: File Parquet tháng 12 & File Map Item
PARQUET_PATH = os.path.join(PROJECT_ROOT, 'data/model_registry/incremental_dec_2025')
MAP_PATH = os.path.join(PROJECT_ROOT, 'data/model_registry/item_map.json')

# Tốc độ giả lập (0 = Max Speed - Bắn nhanh nhất có thể)
SIMULATION_SPEED = 0  

# ==========================================
# 2. AVRO SCHEMA (ĐỊNH NGHĨA CẤU TRÚC TIN NHẮN)
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

# ==========================================
# 3. HÀM LOAD & XỬ LÝ DATA (STRICT MODE)
# ==========================================
def load_and_prepare_data():
    """
    Đọc Parquet, Map ID ngược lại thành String ASIN.
    Chỉ giữ lại Item có trong Map. Tạo View + Purchase.
    """
    print(f"📥 Đang load dữ liệu nguồn từ: {PARQUET_PATH}")
    if not os.path.exists(PARQUET_PATH):
        raise FileNotFoundError(f"❌ Không tìm thấy file data: {PARQUET_PATH}")

    # 1. Đọc dữ liệu mô phỏng (User tháng 12)
    df = pd.read_parquet(PARQUET_PATH)
    
    # 2. Đọc Map (Index -> Item ID thật)
    print(f"📖 Đang đọc Item Map từ: {MAP_PATH}")
    if not os.path.exists(MAP_PATH):
        raise FileNotFoundError(f"❌ Không tìm thấy file Map: {MAP_PATH}")

    with open(MAP_PATH, 'r') as f:
        item_map = json.load(f)
    
    # Chuyển Key từ String sang Int để tra cứu (Vì sequence_ids trong Parquet là int)
    id_to_asin = {int(k): v for k, v in item_map.items()}

    print("💥 Đang xử lý: Kiểm tra Map & Nhân bản data (1 Purchase -> Thêm 1 View)...")
    
    all_events = []
    skipped_count = 0
    valid_items_count = 0
    
    # Duyệt qua từng User trong file Parquet
    for _, row in df.iterrows():
        user_id = row['user_id']
        items = row['sequence_ids']          # List các item index [10, 20, 30]
        timestamps = row['sequence_timestamps'] # List timestamp tương ứng
        
        # Duyệt qua từng hành động trong chuỗi của User đó
        for item_idx, ts in zip(items, timestamps):
            
            # 🔥 [QUAN TRỌNG]: STRICT CHECK
            # Nếu Item Index trong data mới KHÔNG CÓ trong Map cũ -> BỎ QUA LUÔN
            # Để tránh lỗi hệ thống hoặc gợi ý item rác
            if item_idx not in id_to_asin:
                skipped_count += 1
                continue 
            
            # Lấy Item ID thực (Ví dụ: "B00123AB")
            real_item_id = id_to_asin[item_idx]
            valid_items_count += 1
            
            # --- TẠO SỰ KIỆN VIEW (Giả lập: User xem hàng trước khi mua 10-120s) ---
            # Việc này giúp User cũ có thêm dữ liệu tương tác
            delay_seconds = np.random.randint(10, 120)
            all_events.append({
                "user_id": user_id,
                "item_id": real_item_id,
                "timestamp": ts - delay_seconds,
                "event_type": "view",       # Loại sự kiện View
                "rating": 0.0
            })
            
            # --- TẠO SỰ KIỆN PURCHASE (Hành vi mua thật trong Data) ---
            all_events.append({
                "user_id": user_id,
                "item_id": real_item_id,
                "timestamp": ts,
                "event_type": "purchase",   # Loại sự kiện Purchase
                "rating": 5.0
            })

    # Chuyển List thành DataFrame để dễ xử lý
    df_flat = pd.DataFrame(all_events)
    
    # Sắp xếp theo thời gian tăng dần (Mô phỏng dòng chảy thời gian thực)
    if not df_flat.empty:
        df_flat = df_flat.sort_values(by="timestamp").reset_index(drop=True)
    
    print("-" * 60)
    print(f"⚠️  Đã bỏ qua (Skipped): {skipped_count} items (Do ID không có trong Map)")
    print(f"✅  Hợp lệ (Valid):      {valid_items_count} items gốc")
    print(f"🚀  Tổng Events sẽ gửi:  {len(df_flat)} (Bao gồm cả View & Purchase)")
    print("-" * 60)
    
    return df_flat

def delivery_report(err, msg):
    """Callback gọi khi Kafka nhận tin thành công/thất bại"""
    if err is not None:
        print(f"❌ Gửi thất bại: {err}")
    # else:
    #     print(f"✅ Gửi thành công: {msg.key().decode('utf-8')}")

# ==========================================
# 4. MAIN LOOP (GỬI TIN VÀO KAFKA)
# ==========================================
def main():
    # Cấu hình Schema Registry
    schema_registry_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
    avro_serializer = AvroSerializer(schema_registry_client, VALUE_SCHEMA_STR)
    
    # Cấu hình Producer (Tối ưu cho tốc độ cao)
    producer_conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP,
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': avro_serializer,
        'queue.buffering.max.messages': 500000, # Bộ đệm lớn
        'linger.ms': 10,       # Gom batch mỗi 10ms (giúp gửi nhanh hơn gửi lẻ tẻ)
        'compression.type': 'snappy' # Nén dữ liệu để tiết kiệm băng thông
    }
    producer = SerializingProducer(producer_conf)

    # 1. Load và Chuẩn bị dữ liệu
    try:
        df_stream = load_and_prepare_data()
    except Exception as e:
        print(f"❌ Lỗi Data Preparation: {e}")
        return

    total_records = len(df_stream)
    if total_records == 0:
        print("⚠️ Không có data hợp lệ để gửi. Kiểm tra lại file Incremental!")
        return

    print(f"🚀 BẮT ĐẦU SIMULATION: Đang gửi {total_records} events vào topic '{TOPIC}'...")
    
    start_time = time.time()
    
    # 2. Vòng lặp gửi tin
    for i, row in df_stream.iterrows():
        
        # Chuyển timestamp số sang ISO String (cho đúng schema Avro)
        ts_iso = datetime.fromtimestamp(row['timestamp']).isoformat()
        
        value_obj = {
            "event_id": str(uuid.uuid4()),
            "session_id": str(uuid.uuid4()),
            "user_id": str(row['user_id']),
            "item_id": str(row['item_id']), # Đảm bảo là string
            "event_type": row['event_type'],
            "rating_original": float(row['rating']),
            "timestamp": ts_iso,
            "context": {
                "device": "desktop",
                "location": "Simulation_Dec2025",
                "ip": "127.0.0.1"
            }
        }

        # Gửi Async (Không chờ phản hồi từng tin)
        producer.produce(
            topic=TOPIC,
            key=str(row['user_id']),
            value=value_obj,
            on_delivery=delivery_report
        )
        
        # Poll nhẹ để Kafka client xử lý callback (tránh tràn RAM local)
        # 5000 tin mới poll 1 lần để tăng tốc
        if i % 5000 == 0:
            producer.poll(0)
            elapsed = time.time() - start_time
            rate = i / elapsed if elapsed > 0 else 0
            print(f"📤 Đã gửi {i}/{total_records} events... (Tốc độ: {int(rate)} msg/s)")

        # Nếu muốn giả lập tốc độ chậm thì uncomment dòng dưới
        if SIMULATION_SPEED > 0:
            time.sleep(SIMULATION_SPEED)

    # 3. Kết thúc
    print("⏳ Đang đẩy nốt các tin còn lại trong hàng đợi (Flushing)...")
    producer.flush()
    
    duration = time.time() - start_time
    print(f"🎉 HOÀN TẤT! Đã gửi {total_records} events trong {duration:.2f}s.")

if __name__ == "__main__":
    main()
# import time
# import json
# import os
# import uuid
# import pandas as pd
# import numpy as np
# from datetime import datetime

# # Kafka
# from confluent_kafka import SerializingProducer
# from confluent_kafka.serialization import StringSerializer
# from confluent_kafka.schema_registry import SchemaRegistryClient
# from confluent_kafka.schema_registry.avro import AvroSerializer

# # ======================================================
# # 1. CONFIG
# # ======================================================
# KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092')
# SCHEMA_REGISTRY_URL = os.getenv('SCHEMA_REGISTRY_URL', 'http://localhost:9081')
# TOPIC = 'raw_clicks_avro'

# BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# PROJECT_ROOT = os.path.dirname(os.path.dirname(BASE_DIR))

# PARQUET_PATH = os.path.join(PROJECT_ROOT, 'data/model_registry/incremental_dec_2025')
# MAP_PATH = os.path.join(PROJECT_ROOT, 'data/model_registry/item_map.json')

# # 🔥 Simulation Control
# SIMULATION_SPEED = 0        # 0 = theo timestamp thật | >0 = sleep cố định (giây)
# ENABLE_NOISE = True

# # ======================================================
# # 2. AVRO SCHEMA
# # ======================================================
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
#       "type": "record",
#       "name": "ContextData",
#       "fields": [
#         {"name": "device", "type": "string"},
#         {"name": "location", "type": "string"},
#         {"name": "ip", "type": "string"}
#       ]
#     }}
#   ]
# }
# """

# # ======================================================
# # 3. LOAD & PREPARE DATA
# # ======================================================
# def load_and_prepare_data():
#     print(f"📥 Load parquet: {PARQUET_PATH}")
#     df = pd.read_parquet(PARQUET_PATH)

#     print(f"📖 Load item map: {MAP_PATH}")
#     with open(MAP_PATH, 'r') as f:
#         item_map = json.load(f)

#     # index -> ASIN
#     id_to_asin = {int(k): v for k, v in item_map.items()}
#     all_valid_item_ids = list(id_to_asin.values())

#     all_events = []
#     skipped = 0

#     for _, row in df.iterrows():
#         user_id = row['user_id']
#         items = row['sequence_ids']
#         timestamps = row['sequence_timestamps']

#         for item_idx, ts in zip(items, timestamps):
#             if item_idx not in id_to_asin:
#                 skipped += 1
#                 continue

#             real_item = id_to_asin[item_idx]

#             # 🌪 NOISE
#             if ENABLE_NOISE:
#                 for _ in range(np.random.randint(2, 5)):
#                     noise_item = np.random.choice(all_valid_item_ids)
#                     if noise_item == real_item:
#                         continue

#                     noise_ts = ts - np.random.randint(120, 600)
#                     all_events.append({
#                         "user_id": user_id,
#                         "item_id": noise_item,
#                         "timestamp": noise_ts,
#                         "event_type": "view",
#                         "rating": 0.0
#                     })

#             # 🎯 REAL EVENTS
#             all_events.append({
#                 "user_id": user_id,
#                 "item_id": real_item,
#                 "timestamp": ts - np.random.randint(10, 120),
#                 "event_type": "view",
#                 "rating": 0.0
#             })

#             all_events.append({
#                 "user_id": user_id,
#                 "item_id": real_item,
#                 "timestamp": ts,
#                 "event_type": "purchase",
#                 "rating": 5.0
#             })

#     df_events = pd.DataFrame(all_events)
#     df_events = df_events.sort_values("timestamp").reset_index(drop=True)

#     print("-" * 60)
#     print(f"⚠️ Skipped (no map): {skipped}")
#     print(f"🚀 Total events: {len(df_events)}")
#     print("-" * 60)

#     return df_events

# # ======================================================
# # 4. DELIVERY CALLBACK
# # ======================================================
# def delivery_report(err, msg):
#     if err is not None:
#         print(f"❌ Delivery failed: {err}")

# # ======================================================
# # 5. MAIN
# # ======================================================
# def main():
#     schema_registry_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
#     avro_serializer = AvroSerializer(schema_registry_client, VALUE_SCHEMA_STR)

#     # 🔥 PRODUCER ÉP REALTIME
#     producer_conf = {
#         'bootstrap.servers': KAFKA_BOOTSTRAP,
#         'key.serializer': StringSerializer('utf_8'),
#         'value.serializer': avro_serializer,
#         'linger.ms': 0,
#         'batch.num.messages': 1,
#         'queue.buffering.max.messages': 10,
#         'acks': 'all'
#     }

#     producer = SerializingProducer(producer_conf)

#     df_stream = load_and_prepare_data()
#     if df_stream.empty:
#         print("⚠️ No data to stream")
#         return

#     print(f"🚀 START STREAMING TO TOPIC: {TOPIC}")
#     start_time = time.time()
#     prev_ts = None

#     for i, row in df_stream.iterrows():
#         # ⏱️ REAL TIME DELAY
#         if SIMULATION_SPEED > 0:
#             time.sleep(SIMULATION_SPEED)
#         elif prev_ts is not None:
#             delta = row['timestamp'] - prev_ts
#             if delta > 0:
#                 time.sleep(min(delta, 2))  # cap 2s

#         prev_ts = row['timestamp']

#         ts_iso = datetime.fromtimestamp(row['timestamp']).isoformat()

#         value_obj = {
#             "event_id": str(uuid.uuid4()),
#             "session_id": str(uuid.uuid4()),
#             "user_id": str(row['user_id']),
#             "item_id": str(row['item_id']),
#             "event_type": row['event_type'],
#             "rating_original": float(row['rating']),
#             "timestamp": ts_iso,
#             "context": {
#                 "device": "desktop",
#                 "location": "VN_Simulation",
#                 "ip": "192.168.1.1"
#             }
#         }

#         producer.produce(
#             topic=TOPIC,
#             key=str(row['user_id']),
#             value=value_obj,
#             on_delivery=delivery_report
#         )

#         # 🔥 GỬI NGAY TỪNG EVENT
#         producer.poll(0)
#         producer.flush(timeout=1.0)

#         if i % 500 == 0:
#             elapsed = time.time() - start_time
#             print(f"📤 Sent {i}/{len(df_stream)} | {int(i / max(elapsed,1))} msg/s")

#     producer.flush()
#     print(f"🎉 DONE! Sent {len(df_stream)} events")

# # ======================================================
# if __name__ == "__main__":
#     main()
