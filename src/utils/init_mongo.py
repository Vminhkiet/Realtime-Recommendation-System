import json
import os
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

# --- CẤU HÌNH KẾT NỐI ---
# Tự động nhận diện: Nếu chạy trong Docker thì dùng 'mongo', chạy ngoài thì dùng 'localhost'
MONGO_HOST = os.getenv("MONGO_HOST", "mongo")
MONGO_PORT = int(os.getenv("MONGO_PORT", 27017))
MONGO_URI = f"mongodb://{MONGO_HOST}:{MONGO_PORT}/"

DB_NAME = "ecommerce_db"
REVIEW_COLLECTION = "reviews"

# Đường dẫn file (Lưu ý: Đảm bảo path này đúng trong Container)
REVIEW_FILE_PATH = "data/raw_source/Video_Games.jsonl" 

def load_reviews():
    try:
        print(f"🔌 Đang kết nối MongoDB tại: {MONGO_URI}")
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000) # Timeout 5s cho nhanh
        db = client[DB_NAME]
        col = db[REVIEW_COLLECTION]
        
        # Test kết nối
        client.server_info()
        print(f"✅ Kết nối thành công: {DB_NAME}.{REVIEW_COLLECTION}")
    except Exception as e:
        print(f"❌ Lỗi kết nối MongoDB: {e}")
        print("💡 Gợi ý: Kiểm tra xem container 'mongo' có đang chạy không?")
        return

    # --- 1. XÓA DỮ LIỆU CŨ (RESET) ---
    print("🗑  Đang xóa dữ liệu cũ (Clean start)...")
    col.drop() # Xóa sạch collection để nạp lại từ đầu

    # --- 2. TẠO INDEX ---
    print("🛠  Đang tạo Index mới...")
    # Index cho AI (User History)
    col.create_index([("user_id", 1), ("timestamp", 1)])
    # Index cho trang sản phẩm (Sort theo helpful)
    col.create_index([("parent_asin", 1), ("helpful_vote", -1)])

    if not os.path.exists(REVIEW_FILE_PATH):
        print(f"❌ Không tìm thấy file data: {REVIEW_FILE_PATH}")
        print("⚠️ Hãy kiểm tra lại volume mapping trong docker-compose.yml")
        return

    print(f"🚀 Bắt đầu nạp Reviews từ: {REVIEW_FILE_PATH}")

    batch_data = []
    BATCH_SIZE = 5000 # Tăng lên 5000 cho nhanh
    count = 0

    with open(REVIEW_FILE_PATH, "r") as f:
        for line in f:
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue

            user_id = item.get("user_id")
            p_asin = item.get("parent_asin")
            
            if not user_id or not p_asin:
                continue

            doc = {
                "user_id": user_id,
                "parent_asin": p_asin,
                "asin": item.get("asin"),
                "title": item.get("title", ""),
                "text": item.get("text", ""),
                "rating": float(item.get("rating", 0.0)),
                "timestamp": item.get("timestamp"),
                "verified_purchase": item.get("verified_purchase", False),
                "helpful_vote": int(item.get("helpful_vote", 0)),
                "images": item.get("images", [])
            }

            batch_data.append(doc)

            # Insert theo Batch
            if len(batch_data) >= BATCH_SIZE:
                try:
                    col.insert_many(batch_data, ordered=False)
                    count += len(batch_data)
                    print(f"📥 Đã nạp {count} reviews...", end="\r")
                except BulkWriteError as bwe:
                    print(f"⚠️ Lỗi Bulk Write (có thể bỏ qua): {bwe}")
                batch_data = []

    # Insert phần còn lại
    if batch_data:
        try:
            col.insert_many(batch_data, ordered=False)
            count += len(batch_data)
        except BulkWriteError:
            pass

    print(f"\n🎉 HOÀN TẤT! Tổng cộng {count} reviews hiện có trong MongoDB.")
    client.close()

if __name__ == "__main__":
    load_reviews()