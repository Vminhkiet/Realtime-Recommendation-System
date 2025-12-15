import json
import time
import uuid
import random
import os
from datetime import datetime
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from faker import Faker

# --- CẤU HÌNH ---
# Lưu ý: Chạy từ ngoài thì dùng localhost, chạy trong docker thì dùng tên container
SCHEMA_REGISTRY_URL = 'http://schema-registry:8081'
BOOTSTRAP_SERVERS = 'kafka:29092'
TOPIC = 'user_clicks'
SCHEMA_FILE = 'src/simulation/schema/click_schema.avsc'

# Fake Data Generator
fake = Faker('vi_VN')
DEVICES = ['iPhone 14', 'Samsung S23', 'Macbook Air', 'Windows PC', 'iPad']
LOCATIONS = ['Ho Chi Minh', 'Ha Noi', 'Da Nang', 'Can Tho', 'Hai Phong']

# 1. Đọc Schema từ file
with open(SCHEMA_FILE, "r") as f:
    schema_str = f.read()

# 2. Kết nối Schema Registry
schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# 3. Tạo Avro Serializer
avro_serializer = AvroSerializer(schema_registry_client,
                                 schema_str,
                                 lambda obj, ctx: obj) # Hàm chuyển đổi object (ở đây giữ nguyên)

# 4. Cấu hình Producer
producer_conf = {'bootstrap.servers': BOOTSTRAP_SERVERS}
producer = Producer(producer_conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Gửi lỗi: {err}")
    else:
        print(f"✅ Đã gửi Avro: {msg.topic()} [{msg.partition()}]")

def main():
    print(f"🚀 Bắt đầu gửi Avro Data vào topic {TOPIC}...")
    
    while True:
        # Tạo dữ liệu giả
        user_id = f"U{random.randint(1000, 9999)}"
        item_id = f"ITEM_{random.randint(1, 100)}"
        
        data = {
            "event_id": str(uuid.uuid4()),
            "session_id": str(uuid.uuid4()),
            "user_id": user_id,
            "item_id": item_id,
            "event_type": random.choice(['view', 'click', 'purchase']),
            "rating_original": round(random.uniform(1, 5), 1),
            "timestamp": datetime.utcnow().isoformat(),
            "context": {
                "device": random.choice(DEVICES),
                "location": random.choice(LOCATIONS),
                "ip": fake.ipv4()
            }
        }

        # Gửi tin nhắn (Serialize Value sang Avro)
        try:
            producer.produce(topic=TOPIC,
                             key=StringSerializer('utf_8')(str(user_id)),
                             value=avro_serializer(data, SerializationContext(TOPIC, MessageField.VALUE)),
                             on_delivery=delivery_report)
            
            # Flush liên tục để thấy log ngay
            producer.poll(0)
            time.sleep(1) 
            
        except Exception as e:
            print(f"❌ Lỗi Serialization: {e}")
            time.sleep(1)

if __name__ == '__main__':
    main()