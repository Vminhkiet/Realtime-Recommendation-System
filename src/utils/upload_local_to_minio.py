import os
import json
import boto3
from botocore.exceptions import NoCredentialsError

# ================== CẤU HÌNH ==================
MINIO_ENDPOINT = "http://minio:9000" 
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
    if not os.path.exists(local_folder):
        print(f"⚠️ Thư mục không tồn tại, bỏ qua: {local_folder}")
        return

    print(f"📂 Đang quét thư mục: {local_folder} ...")
    
    for root, dirs, files in os.walk(local_folder):
        for filename in files:
            # Đường dẫn file trên máy local
            local_path = os.path.join(root, filename)
            
            # Tính toán đường dẫn tương đối để tạo Key trên S3
            relative_path = os.path.relpath(local_path, local_folder)
            s3_key = os.path.join(s3_prefix, relative_path).replace("\\", "/") # Fix lỗi đường dẫn Windows
            
            try:
                s3_client.upload_file(local_path, BUCKET_NAME, s3_key)
            except Exception as e:
                print(f"  ❌ Lỗi file {filename}: {e}")
    print(f"✅ Đã upload xong folder vào: s3://{BUCKET_NAME}/{s3_prefix}")

# ================== HÀM PATCH CONFIG (MỚI BỔ SUNG) ==================
def patch_model_config(s3_client):
    """
    Tải file config về, sửa latest_model_path, rồi upload lại
    """
    config_key = "model_registry/model_meta_config.json"
    local_temp_config = "temp_config.json"
    
    print("\n--- 5. CẬP NHẬT FILE CONFIG (AUTO-PATCH) ---")
    try:
        # 1. Tải file config hiện tại về (nếu có)
        try:
            s3_client.download_file(BUCKET_NAME, config_key, local_temp_config)
            with open(local_temp_config, 'r') as f:
                config = json.load(f)
        except:
            print("⚠️ Không tìm thấy config trên S3, tạo mới.")
            config = {}

        # 2. Cập nhật đường dẫn model gốc
        target_path = f"s3://datalake/model_registry/sasrec_v1.keras"
        config["latest_model_path"] = target_path
        
        # 3. Ghi lại và Upload
        with open(local_temp_config, 'w') as f:
            json.dump(config, f, indent=4)
            
        s3_client.upload_file(local_temp_config, BUCKET_NAME, config_key)
        print(f"✅ Đã cập nhật 'latest_model_path' = {target_path}")
        
        # Dọn dẹp file tạm
        if os.path.exists(local_temp_config):
            os.remove(local_temp_config)
            
    except Exception as e:
        print(f"❌ Lỗi khi patch config: {e}")

def main():
    # 1. Khởi tạo kết nối
    s3 = boto3.client('s3',
                      endpoint_url=MINIO_ENDPOINT,
                      aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY)
    
    print(f"🚀 BẮT ĐẦU ĐỒNG BỘ DỮ LIỆU LÊN MINIO ({MINIO_ENDPOINT})...")

    # =========================================================
    # 2. UPLOAD CÁC FILE CẤU HÌNH & MAP (JSON/PKL)
    # =========================================================
    print("\n--- 1. UPLOAD METADATA & MAPS ---")
    files_to_upload = [
        "item_map.json", 
        "category_map.json", 
        "item_category.json",
        "model_meta_config.json", 
        "test_set.pkl",
        "sasrec_v1.keras",
        "sasrec_v2.keras"
    ]
    
    for fname in files_to_upload:
        local_path = os.path.join(LOCAL_MODEL_REGISTRY, fname)
        if os.path.exists(local_path):
            s3.upload_file(local_path, BUCKET_NAME, f"model_registry/{fname}")
            print(f"✅ OK: {fname}")
        else:
            print(f"⚠️ Missing: {fname}")

    # =========================================================
    # 3. UPLOAD CHECKPOINT MODEL
    # =========================================================
    print("\n--- 2. UPLOAD MODEL CHECKPOINT ---")
    ckpt_source = os.path.join(LOCAL_MODEL_REGISTRY, "sasrec_v1.keras")
    ckpt_dest = "model_registry/checkpoints/sasrec_latest.keras" 
    
    if os.path.exists(ckpt_source):
        print(f"⬆️ Uploading: sasrec_v1.keras -> {ckpt_dest}")
        s3.upload_file(ckpt_source, BUCKET_NAME, ckpt_dest)
        print("✅ Checkpoint Uploaded!")
    else:
        print("⚠️ Không tìm thấy 'sasrec_v1.keras'.")

    # =========================================================
    # 4. UPLOAD DỮ LIỆU (PARQUET)
    # =========================================================
    print("\n--- 3. UPLOAD DATASET (OLD & NEW) ---")
    
    # upload_folder_to_s3(s3, 
    #                     os.path.join(LOCAL_MODEL_REGISTRY, "processed_parquet"), 
    #                     "processed_parquet")
    
    # upload_folder_to_s3(s3, 
    #                     os.path.join(LOCAL_MODEL_REGISTRY, "incremental_dec_2025"), 
    #                     "incremental_dec_2025")

    # =========================================================
    # 5. UPLOAD MODEL SERVING
    # =========================================================
    print("\n--- 4. UPLOAD MODEL SERVING (EXPORTED) ---")
    local_model_path = os.path.join(LOCAL_MODEL_REGISTRY, "1")
    
    if os.path.exists(local_model_path):
        # upload_folder_to_s3(s3, local_model_path, "models/sasrec/1")
        print("✅ Serving Model Uploaded!")
    else:
        print("⚠️ Không tìm thấy folder model '1'.")

    # =========================================================
    # 6. GỌI HÀM PATCH CONFIG
    # =========================================================
    patch_model_config(s3)

    print("\n🎉🎉🎉 HOÀN TẤT ĐỒNG BỘ!")

if __name__ == "__main__":
    main()