import os
import json
import s3fs
import numpy as np
import pandas as pd
import tensorflow as tf
from tqdm import tqdm
from datetime import datetime
import math
import re

# --- CẤU HÌNH ---
MINIO_CONF = {
    "key": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    "secret": os.getenv("MINIO_SECRET_KEY", "minioadmin"),
    "client_kwargs": {"endpoint_url": os.getenv("MINIO_ENDPOINT", "http://minio:9000")}
}
BUCKET = "datalake"
# Config path giữ nguyên s3a cũng được, code xử lý bên dưới
META_CONFIG_PATH = f"s3://{BUCKET}/model_registry/model_meta_config.json"

MAX_LEN = 50
TOP_K = 10
QUALITY_THRESHOLD = 0.01

try:
    from model import SasRec
except ImportError:
    import sys
    sys.path.append(os.getcwd())
    from src.ai_core.model import SasRec

def get_fs():
    return s3fs.S3FileSystem(**MINIO_CONF)

def force_s3_protocol(path):
    """
    [NUCLEAR OPTION] Phương pháp 'Cắt và Ghép'.
    Bất chấp đầu vào là s3a://, S3A://, hay dính rác,
    nó sẽ ép về s3:// chuẩn.
    """
    if not path: return ""
    
    # 1. Clean cơ bản
    s = str(path).strip().strip('"').strip("'")
    
    # 2. Nếu có chứa '://' -> Cắt đôi và ghép lại
    if "://" in s:
        # Ví dụ: "s3a://bucket/file" -> parts = ["s3a", "bucket/file"]
        parts = s.split("://", 1) 
        clean_suffix = parts[1]
        new_path = f"s3://{clean_suffix}"
        
        # Chỉ in log nếu có sự thay đổi để debug
        if new_path != s:
            print(f"🔧 Force Convert: '{s}' \n             -> '{new_path}'")
        return new_path
    
    # Nếu không có protocol (đường dẫn local), giữ nguyên
    return s

def load_config():
    fs = get_fs()
    # Config path cũng cần force s3
    config_path = force_s3_protocol(META_CONFIG_PATH)
    
    if fs.exists(config_path):
        with fs.open(config_path, 'r') as f:
            return json.load(f)
    return {}

def update_config_metrics(metrics, status):
    fs = get_fs()
    config_path = force_s3_protocol(META_CONFIG_PATH)
    try:
        with fs.open(config_path, 'r') as f:
            config = json.load(f)
    except:
        config = {}

    config["latest_eval_metrics"] = metrics
    config["eval_status"] = status
    config["last_eval_at"] = datetime.now().isoformat()
    
    with fs.open(config_path, 'w') as f:
        json.dump(config, f, indent=4)
    print(f"✅ Đã cập nhật metrics vào Config.")

def get_item_embedding_matrix(model):
    try:
        return model.item_embedding.embeddings.numpy()
    except:
        try:
            return model.get_layer("item_embedding").get_weights()[0]
        except:
            return model.layers[0].get_weights()[0]

def pad_sequence(seq, max_len):
    seq = seq[-max_len:]
    pad_len = max_len - len(seq)
    seq_padded = seq + [0] * pad_len
    mask = [True] * len(seq) + [False] * pad_len
    return np.array(seq_padded, dtype=np.int32), np.array(mask, dtype=bool)

def calculate_metrics(ranked_ids, target_item, k=10):
    if target_item not in ranked_ids[:k]:
        return 0.0, 0.0
    hr = 1.0
    rank_index = np.where(ranked_ids[:k] == target_item)[0][0]
    ndcg = 1.0 / math.log2(rank_index + 2)
    return hr, ndcg

def main():
    print("="*60)
    print("🚀 BẮT ĐẦU QUY TRÌNH ĐÁNH GIÁ (NUCLEAR FIX)")
    print("="*60)

    # 1. Đọc Config
    config = load_config()
    raw_model_path = config.get("latest_model_path")
    raw_test_path = config.get("test_path")
    
    if not raw_model_path or not raw_test_path:
        print("❌ Thiếu path trong config. Hủy."); return

    # [ÁP DỤNG HÀM FIX MỚI]
    model_path = raw_model_path
    test_path = force_s3_protocol(raw_test_path)

    print(f"🎯 Model (Final): {model_path}")
    print(f"🎯 Data  (Final): {test_path}")

    # 2. Load Model
    fs = get_fs()
    local_model_path = "/tmp/current_eval_model.keras"
    
    try:
        if fs.exists(model_path):
            print("⬇️  Đang tải model...")
            fs.get(model_path, local_model_path)
            model = tf.keras.models.load_model(local_model_path, custom_objects={'SasRec': SasRec}, compile=False)
            print("✅ Load Model thành công!")
        else:
            print(f"❌ KHÔNG TÌM THẤY FILE: {model_path}")
            print(f"   (Path gốc trong config là: {repr(raw_model_path)})")
            return
    except Exception as e:
        print(f"❌ Lỗi tải/load model: {e}"); return

    # 3. Vocab Size
    try:
        all_item_embeddings = get_item_embedding_matrix(model)
        model_vocab_size = all_item_embeddings.shape[0]
        print(f"ℹ️  Vocab Size: {model_vocab_size}")
    except Exception as e:
        print(f"❌ Lỗi embedding: {e}"); return

    # 4. Load Data
    try:
        df_test = pd.read_parquet(test_path, storage_options=MINIO_CONF)
        print(f"📊 Loaded {len(df_test)} users.")
    except Exception as e:
        print(f"❌ Lỗi load data: {e}"); return

    # 5. Eval Loop
    print("\n⚙️  Đang chấm điểm...")
    total_hr = 0
    total_ndcg = 0
    valid_users = 0
    debug_printed = False

    for _, row in tqdm(df_test.iterrows(), total=len(df_test)):
        seq = row['sequence_ids']
        if len(seq) < 2: continue
        target = int(seq[-1])

        if target >= model_vocab_size: continue 

        safe_hist = [x if x < model_vocab_size else 0 for x in seq[:-1]]
        p_seq, p_mask = pad_sequence(safe_hist, MAX_LEN)
        
        inputs = {
            "item_ids": tf.expand_dims(p_seq, 0),
            "category_ids": tf.expand_dims(np.zeros_like(p_seq), 0),
            "padding_mask": tf.expand_dims(p_mask, 0)
        }

        try:
            out = model(inputs, training=False)
            if isinstance(out, dict):
                user_vec = out.get("item_sequence_embedding", list(out.values())[0])[0, -1, :]
            else:
                user_vec = out[0, -1, :]

            scores = np.matmul(all_item_embeddings, user_vec)
            scores[0] = -np.inf

            top_idx = np.argpartition(scores, -TOP_K)[-TOP_K:]
            sorted_top = top_idx[np.argsort(scores[top_idx])][::-1]

            hr, ndcg = calculate_metrics(sorted_top, target, TOP_K)
            total_hr += hr
            total_ndcg += ndcg
            valid_users += 1
        except Exception as e:
            if not debug_printed:
                print(f"❌ Debug Error: {e}")
                debug_printed = True
            continue

    # 6. Report
    print("\n" + "="*30)
    print(f"✅ Valid Users: {valid_users}/{len(df_test)}")
    if valid_users > 0:
        avg_hr = total_hr / valid_users
        avg_ndcg = total_ndcg / valid_users
        print(f"🏆 HR@{TOP_K}: {avg_hr:.4f}")
        print(f"🏆 NDCG@{TOP_K}: {avg_ndcg:.4f}")
        update_config_metrics({"HR@10": avg_hr, "NDCG@10": avg_ndcg}, "PASSED" if avg_hr >= QUALITY_THRESHOLD else "FAILED")
    else:
        print("⚠️ Không có kết quả.")
        update_config_metrics({"error": "no_valid_users"}, "FAILED")

if __name__ == "__main__":
    main()