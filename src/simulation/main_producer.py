import json
import time
import uuid
import random
import os
from datetime import datetime # <--- Dùng cái này để tạo giờ chuẩn
from kafka import KafkaProducer
from faker import Faker

# --- 1. CẤU HÌNH (CONFIG) ---
INPUT_FILE = 'data/raw_source/All_Beauty.jsonl' 
KAFKA_TOPIC = 'user_clicks'

# Đọc từ biến môi trường
BOOTSTRAP_SERVERS = os.getenv('KAFKA_SERVER', 'localhost:9092')

# Cấu hình giả lập
fake = Faker('vi_VN') 
DEVICES = ['iPhone 14', 'Samsung S23', 'Macbook Air', 'Windows PC', 'iPad']
LOCATIONS = ['Ho Chi Minh', 'Ha Noi', 'Da Nang', 'Can Tho', 'Hai Phong']

def normalize_data(raw_data):
    try:
        rating = float(raw_data.get('rating', 0.0))
        user_id = raw_data.get('user_id') or raw_data.get('reviewerID')
        if not user_id:
            return None, None, None
        item_id = raw_data.get('asin') or raw_data.get('parent_asin', "unknown")
        return user_id, item_id, rating
    except Exception:
        return None, None, None

def augment_data(rating):
    if rating >= 5.0: event = 'purchase'
    elif rating >= 4.0: event = 'add_to_cart'
    elif rating >= 3.0: event = 'view'
    else: event = 'skip'

    device = random.choice(DEVICES)
    location = random.choice(LOCATIONS)
    ip = fake.ipv4()
    
    return event, device, location, ip

def main():
    print(f"⏳ Đang kết nối tới Kafka tại: {BOOTSTRAP_SERVERS}...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print(f"✅ Kết nối Kafka thành công!")
    except Exception as e:
        print(f"❌ Lỗi kết nối Kafka: {e}")
        return

    print(f"🚀 Đang đọc file: {INPUT_FILE}")

    try:
        with open(INPUT_FILE, 'r') as f:
            for line in f:
                try:
                    raw_record = json.loads(line)
                    
                    # 1. Chuẩn hóa
                    user_id, item_id, rating = normalize_data(raw_record)
                    if not user_id: continue

                    # 2. Session
                    session_id = str(uuid.uuid4())

                    # 3. Augmentation
                    event_type, device, location, ip = augment_data(rating)

                    # [SỬA QUAN TRỌNG] Đổi timestamp sang String ISO 8601
                    # Database chỉ hiểu định dạng này, không hiểu số int
                    current_ts = datetime.utcnow().isoformat() 

                    # 4. Payload
                    message = {
                        "event_id": str(uuid.uuid4()),
                        "session_id": session_id,
                        "user_id": user_id,
                        "item_id": item_id,
                        "event_type": event_type,
                        "rating_original": rating,
                        "timestamp": current_ts, # <--- Đã sửa thành String
                        "context": {
                            "device": device,
                            "location": location,
                            "ip": ip
                        }
                    }

                    # Gửi vào Kafka
                    producer.send(KAFKA_TOPIC, message)
                    
                    print(f"✅ Sent: {user_id[:10]}... | {event_type.upper()} | {location}")
                    
                    # Giả lập độ trễ
                    time.sleep(random.uniform(0.1, 0.5))

                except json.JSONDecodeError:
                    continue
                    
    except FileNotFoundError:
        print(f"❌ Không tìm thấy file: {INPUT_FILE}")

if __name__ == "__main__":
    main()