import json
import os
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

# --- CẤU HÌNH ---
# Nếu chạy qua Docker (Make setup) thì host là 'mongo'
# Nếu chạy ngoài thì dùng localhost
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017/")

DB_NAME = "ecommerce_db"
COLLECTION_NAME = "products"

META_FILE_PATH = "data/raw_source/meta_All_Beauty.jsonl"


def load_data():
    # Kết nối MongoDB
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        col = db[COLLECTION_NAME]
        print(f"🔌 Đã kết nối MongoDB: {DB_NAME}.{COLLECTION_NAME}")
    except Exception as e:
        print(f"❌ Lỗi kết nối MongoDB: {e}")
        return

    # Index
    print("🛠 Đang tạo Index...")
    col.create_index("asin", unique=True)
    col.create_index("parent_asin")

    # Kiểm tra file
    if not os.path.exists(META_FILE_PATH):
        print(f"❌ Không tìm thấy file: {META_FILE_PATH}")
        return

    print(f"🚀 Bắt đầu nạp dữ liệu từ: {META_FILE_PATH}")

    batch_data = []
    BATCH_SIZE = 1000
    count = 0

    with open(META_FILE_PATH, "r") as f:
        for line in f:
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue

            # Lấy asin / parent_asin
            p_asin = item.get("parent_asin")
            asin = item.get("asin") or p_asin
            if not asin:
                continue

            # Ảnh (ưu tiên ảnh nét nhất)
            image_url = "https://via.placeholder.com/150"

            if item.get("images"):
                first_img = item["images"][0]
                image_url = (
                    first_img.get("hi_res")
                    or first_img.get("large")
                    or first_img.get("thumb")
                    or image_url
                )

            # Giá
            try:
                price = float(item.get("price")) if item.get("price") else 0.0
            except:
                price = 0.0

            # Đóng gói dữ liệu
            doc = {
                "asin": asin,
                "parent_asin": p_asin,
                "title": item.get("title", "Unknown Product"),
                "main_category": item.get("main_category", "Uncategorized"),

                "price": price,
                "image": image_url,
                "store": item.get("store", "Unknown Brand"),
                "average_rating": item.get("average_rating", 0.0),
                "rating_number": item.get("rating_number", 0),

                "features": item.get("features", []),
                "description": item.get("description", []),
                "details": item.get("details", {}),
                "categories": item.get("categories", []),
                "videos": item.get("videos", []),
                "bought_together": item.get("bought_together", []),
            }

            batch_data.append(doc)

            # Batch insert
            if len(batch_data) >= BATCH_SIZE:
                try:
                    col.insert_many(batch_data, ordered=False)
                    count += len(batch_data)
                    print(f"✅ Đã nạp {count} sản phẩm...", end="\r")
                except BulkWriteError:
                    pass

                batch_data = []

    # Insert phần dư cuối cùng
    if batch_data:
        try:
            col.insert_many(batch_data, ordered=False)
            count += len(batch_data)
        except BulkWriteError:
            pass

    print(f"\n🎉 HOÀN TẤT! Tổng cộng {count} sản phẩm đã vào MongoDB.")

    client.close()


if __name__ == "__main__":
    load_data()
