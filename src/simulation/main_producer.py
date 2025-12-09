import json
import time
import uuid
import random
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker

# --- 1. CẤU HÌNH (CONFIG) ---
# Đường dẫn tương đối từ thư mục chạy lệnh (root folder)
INPUT_FILE = 'data/raw_source/All_Beauty.jsonl' 
KAFKA_TOPIC = 'user_clicks'
BOOTSTRAP_SERVERS = 'localhost:9092' # Địa chỉ Kafka trong Docker

# Cấu hình giả lập (Augmentation Config)
fake = Faker('vi_VN') # Fake thông tin Việt Nam
DEVICES = ['iPhone 14', 'Samsung S23', 'Macbook Air', 'Windows PC', 'iPad']
LOCATIONS = ['Ho Chi Minh', 'Ha Noi', 'Da Nang', 'Can Tho', 'Hai Phong']
BROWSERS = ['Chrome', 'Safari', 'Firefox', 'Edge']

def normalize_data(raw_data):
    """
    NHIỆM VỤ: Chuẩn hóa dữ liệu đầu vào, xử lý null, ép kiểu.
    """
    try:
        rating = float(raw_data.get('rating', 0.0))
        
        # Xử lý trường hợp file dùng 'reviewerID' hoặc 'user_id'
        user_id = raw_data.get('user_id') or raw_data.get('reviewerID')
        if not user_id:
            return None, None, None # Bỏ qua dòng lỗi
            
        item_id = raw_data.get('asin') or raw_data.get('parent_asin', "unknown")
        
        return user_id, item_id, rating
    except Exception:
        return None, None, None

def augment_data(rating):
    """
    NHIỆM VỤ: Làm giàu dữ liệu (Data Augmentation)
    Biến đổi Rating tĩnh -> Hành vi động & Ngữ cảnh
    """
    # 1. Biến đổi hành vi (Transformation)
    if rating >= 5.0: event = 'purchase'
    elif rating >= 4.0: event = 'add_to_cart'
    elif rating >= 3.0: event = 'view'
    else: event = 'skip'

    # 2. Thêm ngữ cảnh giả lập (Enrichment)
    device = random.choice(DEVICES)
    location = random.choice(LOCATIONS)
    ip = fake.ipv4()
    
    return event, device, location, ip

def main():
    print("⏳ Đang kết nối tới Kafka...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print(f"✅ Kết nối Kafka thành công tại {BOOTSTRAP_SERVERS}")
    except Exception as e:
        print(f"❌ Lỗi kết nối Kafka: {e}")
        print("💡 Gợi ý: Bạn đã chạy 'docker-compose up' chưa?")
        return

    print(f"🚀 Đang đọc file: {INPUT_FILE}")

    try:
        with open(INPUT_FILE, 'r') as f:
            for line in f:
                try:
                    raw_record = json.loads(line)
                    
                    # --- GIAI ĐOẠN XỬ LÝ (PRE-PROCESSING) ---
                    
                    # 1. Chuẩn hóa
                    user_id, item_id, rating = normalize_data(raw_record)
                    if not user_id: continue

                    # 2. Chia nhỏ / Tạo Session (Sessionization)
                    # Giả lập mỗi event là một phần của 1 session mới
                    session_id = str(uuid.uuid4())

                    # 3. Làm giàu (Augmentation)
                    event_type, device, location, ip = augment_data(rating)

                    # 4. Ghi log thời gian thực (Time Shifting)
                    current_ts = int(time.time() * 1000)

                    # --- ĐÓNG GÓI BẢN TIN (FINAL PAYLOAD) ---
                    message = {
                        "event_id": str(uuid.uuid4()),
                        "session_id": session_id,
                        "user_id": user_id,
                        "item_id": item_id,
                        "event_type": event_type,
                        "rating_original": rating,
                        "timestamp": current_ts, # Quan trọng cho Real-time
                        "context": {
                            "device": device,
                            "location": location,
                            "ip": ip
                        }
                    }

                    # Gửi vào Kafka
                    producer.send(KAFKA_TOPIC, message)
                    
                    # Log ra màn hình để demo
                    print(f"✅ Sent: {user_id[:10]}... | {event_type.upper()} | {location}")
                    
                    # Giả lập độ trễ (Sleep) để giống người thật đang click
                    time.sleep(random.uniform(0.1, 0.5))

                except json.JSONDecodeError:
                    continue
                    
    except FileNotFoundError:
        print(f"❌ Không tìm thấy file: {INPUT_FILE}")
        print("💡 Hãy tải file Amazon về và bỏ vào thư mục data/raw_source/")

if __name__ == "__main__":
    main()