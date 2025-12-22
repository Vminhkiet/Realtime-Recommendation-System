import os
import boto3
from botocore.exceptions import NoCredentialsError

# ================== CẤU HÌNH ==================
MINIO_ENDPOINT = "http://minio:9000" # Dùng localhost vì bạn chạy script này từ máy host
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
BUCKET_NAME = "datalake"

# Đường dẫn Local (Nơi bạn đang có dữ liệu trong VS Code)
LOCAL_MODEL_REGISTRY = "data/model_registry" 

# ================== HÀM UPLOAD ĐỆ QUY ==================
def upload_folder_to_s3(s3_client, local_folder, s3_prefix):
    """
    Upload toàn bộ thư mục local lên MinIO giữ nguyên cấu trúc
    """
    print(f"📂 Đang quét thư mục: {local_folder} ...")
    
    for root, dirs, files in os.walk(local_folder):
        for filename in files:
            # Đường dẫn file trên máy local
            local_path = os.path.join(root, filename)
            
            # Tính toán đường dẫn tương đối để tạo Key trên S3
            relative_path = os.path.relpath(local_path, local_folder)
            s3_key = os.path.join(s3_prefix, relative_path).replace("\\", "/") # Fix lỗi đường dẫn Windows
            
            print(f"  ⬆️ Uploading: {relative_path} -> s3://{BUCKET_NAME}/{s3_key}")
            
            try:
                s3_client.upload_file(local_path, BUCKET_NAME, s3_key)
            except Exception as e:
                print(f"  ❌ Lỗi file {filename}: {e}")

def main():
    # 1. Khởi tạo kết nối
    s3 = boto3.client('s3',
                      endpoint_url=MINIO_ENDPOINT,
                      aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY)

    # 2. Upload các file Map (JSON) & Pickle
    # Những file này nằm ngay trong root của model_registry
    files_to_upload = [
        "item_map.json", 
        "category_map.json", 
        "item_category.json",
        "test_set.pkl" 
        # "sasrec_v1.keras" # File này chỉ dùng để train tiếp, không cần thiết cho Serving nếu đã có folder '1'
    ]
    
    print("--- 🚀 BẮT ĐẦU UPLOAD CÁC FILE LẺ ---")
    for fname in files_to_upload:
        local_path = os.path.join(LOCAL_MODEL_REGISTRY, fname)
        if os.path.exists(local_path):
            s3.upload_file(local_path, BUCKET_NAME, f"model_registry/{fname}")
            print(f"✅ Đã upload: {fname}")
        else:
            print(f"⚠️ Không tìm thấy: {fname}")

    # 3. Upload Folder Dữ liệu Train (Parquet)
    # Upload vào: datalake/processed_parquet
    print("\n--- 📦 BẮT ĐẦU UPLOAD PARQUET ---")
    upload_folder_to_s3(s3, 
                        os.path.join(LOCAL_MODEL_REGISTRY, "processed_parquet"), 
                        "processed_parquet")

    # 4. Upload Model cho TF Serving (Quan trọng nhất)
    # Cấu trúc TF Serving bắt buộc: models/<tên_model>/<version>/saved_model.pb
    # Local của bạn đang là: model_registry/1/...
    # Chúng ta sẽ đẩy lên: models/sasrec/1/...
    print("\n--- 🤖 BẮT ĐẦU UPLOAD MODEL (TF SERVING) ---")
    local_model_path = os.path.join(LOCAL_MODEL_REGISTRY, "1")
    if os.path.exists(local_model_path):
        upload_folder_to_s3(s3, local_model_path, "models/sasrec/1")
        print("✅ Upload Model thành công! Sẵn sàng cho TF Serving.")
    else:
        print("❌ Không tìm thấy thư mục model version '1'. Bạn đã save model chưa?")

if __name__ == "__main__":
    main()