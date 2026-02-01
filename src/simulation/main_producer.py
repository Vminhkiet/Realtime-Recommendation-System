import time
import json
import random
import os
from kafka import KafkaProducer

# ==========================================
# CẤU HÌNH PATH
# ==========================================
# Đảm bảo trỏ đúng vào file mà code train vừa lưu
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(BASE_DIR))

# File chứa User đã train (Phải khớp với code train)
VALID_USERS_PATH = os.path.join(PROJECT_ROOT, 'src/simulation/valid_users.json')
# File chứa Item đã train
ITEM_MAP_PATH = os.path.join(PROJECT_ROOT, 'data/model_registry/item_map.json')

def load_valid_data():
    print(f"📂 Đang đọc User từ: {VALID_USERS_PATH}")
    
    if not os.path.exists(VALID_USERS_PATH):
        print("❌ LỖI: Không tìm thấy valid_users.json. Hãy chạy train model trước!")
        exit()

    with open(VALID_USERS_PATH, 'r') as f:
        users = json.load(f)

    # Load Items để đảm bảo không random ra item lạ
    with open(ITEM_MAP_PATH, 'r') as f:
        item_map = json.load(f)
        # Lấy danh sách Item ID gốc (Key hay Value tùy format map của bạn)
        # Giả sử format: {"ItemID_String": Index_Int}
        items = list(item_map.keys()) 

    print(f"✅ Đã load: {len(users)} Users đã train.")
    print(f"✅ Đã load: {len(items)} Items đã train.")
    return users, items

# ==========================================
# MAIN SIMULATION LOOP
# ==========================================
def main():
    producer = KafkaProducer(
        bootstrap_servers='kafka:29092', # Sửa port nếu cần
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # 1. Load đúng User/Item đã học
    valid_users, valid_items = load_valid_data()

    print("🚀 Bắt đầu giả lập user click...")
    
    try:
        while True:
            # 2. CHỈ CHỌN USER/ITEM TRONG LIST ĐÃ HỌC
            user_id = random.choice(valid_users)
            item_id = random.choice(valid_items)

            # Cần đảm bảo format là string (nếu Kafka/Redis yêu cầu string)
            user_id = str(user_id) 
            item_id = str(item_id)

            message = {
                "user_id": user_id,
                "item_id": item_id,
                "event_type": "click",
                "timestamp": datetime.now().isoformat()
            }

            print(f"📤 Sending: User={user_id} -> Item={item_id}")
            producer.send('user_clicks', message)
            
            time.sleep(1) # Chờ 1 giây bắn 1 lần

    except KeyboardInterrupt:
        print("🛑 Dừng simulation.")

if __name__ == "__main__":
    from datetime import datetime
    main()