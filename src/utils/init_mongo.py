import json
import os
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

# --- CẤU HÌNH ---
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = "ecommerce_db"
REVIEW_COLLECTION = "reviews" # Collection riêng cho review

# Đổi đường dẫn này tới file chứa Review của bạn
REVIEW_FILE_PATH = "data/raw_source/All_Beauty.jsonl" 

def load_reviews():
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        col = db[REVIEW_COLLECTION]
        print(f"🔌 Đã kết nối MongoDB: {DB_NAME}.{REVIEW_COLLECTION}")
    except Exception as e:
        print(f"❌ Lỗi kết nối MongoDB: {e}")
        return

    # --- TẠO INDEX (Rất quan trọng cho tốc độ) ---
    print("🛠 Đang tạo Index cho Reviews...")
    
    # 1. Index hỗ trợ lấy lịch sử user theo thời gian (Cho AI SASRec)
    col.create_index([("user_id", 1), ("timestamp", 1)])
    
    # 2. Index hỗ trợ hiển thị review ở trang chi tiết sản phẩm (Sort theo hữu ích)
    col.create_index([("parent_asin", 1), ("helpful_vote", -1)])

    if not os.path.exists(REVIEW_FILE_PATH):
        print(f"❌ Không tìm thấy file: {REVIEW_FILE_PATH}")
        return

    print(f"🚀 Bắt đầu nạp Reviews từ: {REVIEW_FILE_PATH}")

    batch_data = []
    BATCH_SIZE = 2000 # Review nhẹ hơn metadata, có thể tăng batch
    count = 0

    with open(REVIEW_FILE_PATH, "r") as f:
        for line in f:
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue

            # Lấy các trường quan trọng
            user_id = item.get("user_id")
            p_asin = item.get("parent_asin")
            
            # Bỏ qua nếu dữ liệu lỗi
            if not user_id or not p_asin:
                continue

            # Xử lý images (MongoDB lưu list object rất tốt)
            # Dữ liệu gốc thường là list các dict: [{'small_image_url': '...', ...}]
            images = item.get("images", []) 

            # Đóng gói document
            doc = {
                "user_id": user_id,
                "parent_asin": p_asin,
                "asin": item.get("asin"), # Variant ID
                
                "title": item.get("title", ""),
                "text": item.get("text", ""),
                "rating": float(item.get("rating", 0.0)),
                
                "timestamp": item.get("timestamp"), # Unix timestamp (int)
                "verified_purchase": item.get("verified_purchase", False),
                "helpful_vote": int(item.get("helpful_vote", 0)),
                
                "images": images # Lưu nguyên mảng images vào đây
            }

            batch_data.append(doc)

            # Batch insert
            if len(batch_data) >= BATCH_SIZE:
                try:
                    col.insert_many(batch_data, ordered=False)
                    count += len(batch_data)
                    print(f"✅ Đã nạp {count} reviews...", end="\r")
                except BulkWriteError:
                    pass # Bỏ qua lỗi trùng lặp (nếu có)
                batch_data = []

    # Insert phần dư cuối
    if batch_data:
        try:
            col.insert_many(batch_data, ordered=False)
            count += len(batch_data)
        except BulkWriteError:
            pass

    print(f"\n🎉 HOÀN TẤT! Tổng cộng {count} reviews đã vào MongoDB.")
    client.close()

if __name__ == "__main__":
    load_reviews()