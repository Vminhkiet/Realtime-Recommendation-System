# import os
# import pandas as pd
# from datetime import datetime

# # Đường dẫn folder data mới (Giả lập)
# BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(BASE_DIR)))
# DATA_PATH = os.path.join(PROJECT_ROOT, 'data/model_registry/processed_parquet')

# def main():
#     print(f"🔍 Đang kiểm tra dữ liệu tại: {DATA_PATH}")
    
#     if not os.path.exists(DATA_PATH):
#         print("❌ Folder không tồn tại! Bạn chưa chạy spark_process_simulation.py?"); return

#     try:
#         # Đọc parquet bằng Pandas (nhanh hơn Spark cho việc check)
#         df = pd.read_parquet(DATA_PATH)
        
#         # 1. Kiểm tra số lượng
#         num_users = len(df)
#         print(f"\n📊 TỔNG QUAN:")
#         print(f"   - Tổng số User: {num_users}")
        
#         if num_users == 0:
#             print("⚠️ File rỗng!"); return

#         # 2. Kiểm tra thời gian (Lấy tất cả timestamp ra để tìm Min/Max)
#         all_timestamps = []
#         for seq in df['sequence_timestamps']:
#             all_timestamps.extend(seq)
            
#         min_ts = min(all_timestamps)
#         max_ts = max(all_timestamps)
        
#         print(f"   - Ngày bắt đầu: {datetime.fromtimestamp(min_ts)}")
#         print(f"   - Ngày kết thúc: {datetime.fromtimestamp(max_ts)}")
        
#         # 3. Kiểm tra độ dài chuỗi (Điều kiện >= 2)
#         df['seq_len'] = df['sequence_ids'].apply(len)
#         min_len = df['seq_len'].min()
#         avg_len = df['seq_len'].mean()
        
#         print(f"   - Độ dài chuỗi Min: {min_len} (Phải >= 2)")
#         print(f"   - Độ dài chuỗi TB : {avg_len:.2f}")

#         # 4. Soi mẫu 1 dòng dữ liệu
#         print(f"\n👀 MẪU DỮ LIỆU ĐẦU TIÊN:")
#         row = df.iloc[0]
#         print(f"   - User ID hash: {row['user_id']}")
#         print(f"   - Items: {row['sequence_ids']}")
#         print(f"   - Cats : {row['category_ids']}")
#         print(f"   - Time : {[datetime.fromtimestamp(t).strftime('%Y-%m-%d') for t in row['sequence_timestamps']]}")

#     except Exception as e:
#         print(f"❌ Lỗi khi đọc file: {e}")

# if __name__ == "__main__":
#     main()
# import pandas as pd
# import json
# import os
# import s3fs

# # ==========================================
# # CẤU HÌNH KẾT NỐI S3 (FIXED)
# # ==========================================
# MINIO_OPTS = {
#     "key": "minioadmin",
#     "secret": "minioadmin",
#     "client_kwargs": {"endpoint_url": "http://minio:9000"}
# }

# BUCKET = "datalake"

# def check_data():
#     print("🕵️ ĐANG KIỂM TRA DỮ LIỆU TEST...")
    
#     # Dùng s3fs chỉ để đọc file json config (nhẹ)
#     fs = s3fs.S3FileSystem(**MINIO_OPTS)
    
#     # 1. Lấy đường dẫn Test từ Config
#     try:
#         config_path = f"s3://{BUCKET}/model_registry/model_meta_config.json"
#         with fs.open(config_path, 'r') as f:
#             config = json.load(f)
#             # Chuẩn hóa về s3://
#             test_path = config.get("test_path", "").replace("s3a://", "s3://")
            
#             print(f"📂 Path tìm thấy: {test_path}")
            
#             if not test_path:
#                 print("❌ Config không chứa 'test_path'.")
#                 return
#     except Exception as e:
#         print(f"❌ Lỗi đọc config: {e}")
#         return

#     # 2. Đọc file Parquet (DÙNG STORAGE_OPTIONS THAY VÌ FILESYSTEM)
#     # Đây là chỗ sửa lỗi "outside base dir"
#     try:
#         df = pd.read_parquet(
#             test_path, 
#             storage_options=MINIO_OPTS
#         )
#     except Exception as e:
#         print(f"❌ Lỗi đọc Parquet: {e}")
#         # Gợi ý debug nếu path sai
#         print("👉 Gợi ý: Kiểm tra xem folder trên MinIO có file .parquet không hay chỉ có _SUCCESS?")
#         return

#     # 3. Soi dữ liệu
#     print(f"\n📊 Tổng số User Test: {len(df)}")
    
#     if df.empty:
#         print("⚠️ File rỗng!")
#         return

#     # In mẫu 3 user đầu tiên
#     for index, row in df.head(3).iterrows():
#         print("-" * 30)
#         print(f"👤 User: {row['user_id']}")
        
#         # History
#         hist = row.get('sequence_ids', [])
#         print(f"   🔹 Input (History): ...{hist[-5:]} (Len: {len(hist)})")
        
#         # Future Label
#         future = row.get('ground_truth_items', [])
#         print(f"   🎯 Label (Future):  {future}")
        
#         # Check overlap
#         overlap = set(hist) & set(future)
#         if overlap:
#             print(f"   ✅ Có lặp lại item: {overlap}")
#         else:
#             print(f"   ⚠️ Không trùng item nào (Hành vi thay đổi hoặc ID lệch).")

# if __name__ == "__main__":
#     check_data()

import pandas as pd
import s3fs

# Cấu hình MinIO
MINIO_CONF = {
    "key": "minioadmin", "secret": "minioadmin",
    "client_kwargs": {"endpoint_url": "http://minio:9000"}
}
BUCKET = "datalake"

def check_user_overlap():
    print("🕵️ KIỂM TRA TỶ LỆ USER QUAY LẠI (RETENTION CHECK)...")
    
    try:
        # Đọc file Parquet thô (Processed Clicks)
        # Lưu ý: Cần đường dẫn tới file gốc chứa data cả 2 tuần
        path = f"s3://{BUCKET}/topics/processed_clicks" 
        df = pd.read_parquet(path, storage_options=MINIO_CONF)
        
        # Convert timestamp sang ngày
        df['date'] = pd.to_datetime(df['timestamp'], unit='ms')
        
        # Chia 2 tập
        # Giả sử mốc cắt là ngày 2025-12-15 (Bạn sửa lại ngày theo config của bạn)
        SPLIT_DATE = pd.Timestamp("2025-11-8") 
        
        users_week_1 = set(df[df['date'] < SPLIT_DATE]['user_id'])
        users_week_2 = set(df[df['date'] >= SPLIT_DATE]['user_id'])
        
        print(f"   👥 Users Tuần 1 (Train): {len(users_week_1)}")
        print(f"   👥 Users Tuần 2 (Test):  {len(users_week_2)}")
        
        # Tìm giao nhau (Intersection)
        loyal_users = users_week_1 & users_week_2
        print(f"   ✅ Users Giao thoa (Có thể Test): {len(loyal_users)}")
        
        if len(loyal_users) == 0:
            print("   ❌ CHẾT DỮ LIỆU: Không có user nào hoạt động ở cả 2 tuần -> Model không thể đánh giá!")
        else:
            print(f"   ok Dữ liệu ổn. Có {len(loyal_users)} user để chấm điểm.")
            
    except Exception as e:
        print(f"Lỗi: {e}")

if __name__ == "__main__":
    check_user_overlap()