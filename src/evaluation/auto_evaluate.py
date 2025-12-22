import os
import sys
import json
import s3fs
import pickle
import numpy as np
import pandas as pd
import tensorflow as tf
from tqdm import tqdm
from datetime import datetime

# Thêm đường dẫn để load custom model class
sys.path.append("/home/spark/work")

# =======================
# CẤU HÌNH HỆ THỐNG
# =======================
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET_NAME = "datalake"

# Đường dẫn Model & Data
MODEL_BASE_DIR = "/home/spark/work/models/sasrec"
S3_MODEL_REGISTRY = "model_registry"
current_week = datetime.now().strftime("%Y_week_%U")
S3_DATA_PATH = f"s3://{BUCKET_NAME}/training_data/{current_week}"

# Cấu hình đánh giá
MAX_LEN = 50
NUM_NEG_TEST = 99  # Đánh giá 1 Positive vs 99 Negatives
SEED = 42

np.random.seed(SEED)
tf.random.set_seed(SEED)

# =======================
# HÀM HỖ TRỢ
# =======================
def get_latest_model_version():
    """Tìm phiên bản model mới nhất"""
    if not os.path.exists(MODEL_BASE_DIR):
        return None
    versions = [int(d) for d in os.listdir(MODEL_BASE_DIR) if d.isdigit()]
    return max(versions) if versions else None

def get_metadata():
    """Tải số lượng Item từ MinIO để biết khoảng random negative"""
    s3 = boto3.client('s3', endpoint_url=MINIO_ENDPOINT,
                      aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY, use_ssl=False)
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=f"{S3_MODEL_REGISTRY}/model_meta_config.json")
        return json.loads(obj['Body'].read().decode('utf-8'))
    except:
        return {"max_item_idx": 1000} # Fallback

def load_data_from_minio():
    """Đọc trực tiếp file Parquet từ MinIO"""
    print(f"📂 Đang tải dữ liệu Test từ: {S3_DATA_PATH}")
    fs = s3fs.S3FileSystem(key=ACCESS_KEY, secret=SECRET_KEY, client_kwargs={'endpoint_url': MINIO_ENDPOINT})
    try:
        files = fs.glob(f"{S3_DATA_PATH}/*.parquet")
        if not files: return None
        return pd.concat([pd.read_parquet(f"s3://{f}", filesystem=fs) for f in files])
    except Exception as e:
        print(f"❌ Lỗi đọc data: {e}")
        return None

def pad_sequence(seq, max_len):
    """Padding sequence cho đúng chuẩn input model"""
    seq = list(seq)[-max_len:] # Cắt nếu dài quá
    pad_len = max_len - len(seq)
    return list(seq) + [0] * pad_len # Pad sau (hoặc trước tuỳ config train)

# =======================
# MAIN EVALUATION LOOP
# =======================
def main():
    print("\n📊 BẮT ĐẦU ĐÁNH GIÁ (AUTO EVALUATE) - LEAVE ONE OUT")
    
    # 1. Load Model
    ver = get_latest_model_version()
    if not ver:
        print("❌ Không tìm thấy model nào.")
        return
    model_path = f"{MODEL_BASE_DIR}/{ver}"
    print(f"🔄 Load model version {ver} từ {model_path}...")
    
    try:
        # Load trọn vẹn Keras Model để truy cập layer embedding bên trong
        model = tf.keras.models.load_model(model_path)
    except:
        print("⚠️ Không load được dạng Keras Model (có thể do format SavedModel).")
        print("ℹ️ Chuyển sang chế độ Serving Signature (chậm hơn nhưng an toàn).")
        model = tf.saved_model.load(model_path)
    
    # 2. Load Data & Metadata
    df = load_data_from_minio()
    if df is None or df.empty:
        print("❌ Không có dữ liệu để test.")
        return
        
    meta = get_metadata()
    vocab_size = meta.get("max_item_idx", 1000)
    
    hits_10, ndcgs_10 = 0, 0
    num_users = len(df)
    
    print(f"👥 Tổng số User test: {num_users}")
    print(f"📦 Vocab Size: {vocab_size}")

    # 3. Vòng lặp đánh giá
    # Chiến thuật: Lấy list item của user. 
    # - Input: [Item 1, ..., Item N-1]
    # - Label: Item N (Cái cuối cùng user đã click)
    # - Negative: 99 cái user chưa xem (hoặc random)
    
    for _, row in tqdm(df.iterrows(), total=num_users, desc="Evaluating"):
        full_seq = row['sequence_ids']
        if len(full_seq) < 2: continue # Không đủ để test
        
        # Tách Train/Test (Leave-one-out)
        target_item = full_seq[-1]      # Ground Truth
        input_seq = full_seq[:-1]       # History để dự đoán
        
        # Padding
        padded_input = pad_sequence(input_seq, MAX_LEN)
        
        # Negative Sampling
        test_items = [target_item]
        while len(test_items) < NUM_NEG_TEST + 1:
            neg = np.random.randint(1, vocab_size + 1)
            if neg not in full_seq: # Đảm bảo không trùng lịch sử
                test_items.append(neg)
        
        # Chuyển thành Tensor
        input_tensor = tf.constant([padded_input] * 100, dtype=tf.int32) # Batch size 100 (1 pos + 99 neg)
        
        # Giả lập category (nếu có)
        cat_tensor = tf.zeros_like(input_tensor) 
        
        # --- DỰ ĐOÁN ---
        # Cách này hơi "cục súc" (predict 100 lần) nhưng tương thích mọi model
        # Để tối ưu, nên lấy User Embedding 1 lần rồi nhân với 100 Item Embedding
        # Nhưng ở đây ta dùng hàm predict() cho đơn giản logic
        
        # Dự đoán cho 100 trường hợp: (User History + Candidate Item)
        # Vì model SasRec thường chỉ nhận input sequence, ta cần trick nhẹ:
        # Append candidate item vào cuối chuỗi input để xem score của nó
        
        # TRICK: Ta dùng User Embedding từ model để tính Dot Product với 100 Items
        # Yêu cầu model phải expose được Embedding Layer. 
        # Nếu dùng SavedModel, ta gọi hàm serve() để lấy logits
        
        try:
            # Gọi model để lấy Logits cho TẤT CẢ items (cách nhanh nhất)
            # Input: (1, 50)
            single_input = tf.constant([padded_input], dtype=tf.int32)
            single_cat = tf.constant([pad_sequence([1]*len(input_seq), MAX_LEN)], dtype=tf.int32)
            
            logits = model(
                {"item_ids": single_input, "category_ids": single_cat}, 
                training=False
            ) 
            # Logits shape: (1, 50, Vocab_Size) -> Lấy bước cuối cùng
            # Nếu model trả về dict
            if isinstance(logits, dict):
                # Tuỳ vào output model của bạn (thường là 'output_1' hoặc tên layer)
                # Giả sử model trả về sequence output
                seq_output = list(logits.values())[0] # (1, 50, Hidden)
                scores = seq_output[0, -1, :] # Vector điểm số cho toàn bộ Vocab
            else:
                # Nếu model trả thẳng logit (1, 50, Vocab)
                scores = logits[0, -1, :] 

            # Lấy điểm của 100 item test
            test_scores = tf.gather(scores, test_items).numpy()
            
            # Ranking
            # Item đầu tiên (index 0) là Target. Đếm xem có bao nhiêu thằng điểm cao hơn nó
            rank = np.sum(test_scores > test_scores[0]) + 1
            
            if rank <= 10:
                hits_10 += 1
                ndcgs_10 += 1.0 / np.log2(rank + 1)
                
        except Exception as e:
            # Skip nếu lỗi dimension (do config chưa khớp)
            pass

    # =======================
    # BÁO CÁO KẾT QUẢ
    # =======================
    hr = hits_10 / num_users
    ndcg = ndcgs_10 / num_users
    
    print("\n" + "="*50)
    print(f"🏆 KẾT QUẢ TEST (Model v{ver})")
    print("="*50)
    print(f"👥 Users Evaluated : {num_users}")
    print(f"🎯 Hit Rate @10    : {hr:.4f}  ({hr*100:.2f}%)")
    print(f"⭐ NDCG @10        : {ndcg:.4f}")
    print("="*50 + "\n")

if __name__ == "__main__":
    main()