import os
import json
import pickle
import numpy as np
import tensorflow as tf
import mlflow
from tqdm import tqdm

# =============================================================================
# 1. SETUP CREDENTIALS (MINIO & MLFLOW)
# =============================================================================
# Đảm bảo các thông số này khớp với docker-compose của bạn
os.environ["AWS_ACCESS_KEY_ID"] = "minioadmin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "minioadmin"
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
os.environ["MLFLOW_S3_ENDPOINT_URL"] = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
os.environ["MLFLOW_TRACKING_URI"] = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")

# =============================================================================
# 2. IMPORTS & CONFIG
# =============================================================================
# --- IMPORT MODEL CLASS ---
# Cố gắng import class SasRec. File model.py phải nằm cùng thư mục hoặc trong PYTHONPATH
try:
    from model import SasRec
except ImportError:
    try:
        from src.ai_core.model import SasRec
    except ImportError:
        print("❌ LỖI NGHIÊM TRỌNG: Không tìm thấy class 'SasRec'.")
        print("👉 Hãy đảm bảo file 'model.py' nằm cùng thư mục với script này.")
        import sys; sys.exit(1)

# Cấu hình chung
EXPERIMENT_NAME = "sasrec_original_config_kiet"
DOWNLOAD_DIR = "./downloaded_artifacts"  # Thư mục tạm để lưu file tải về
MAX_LEN = 50       # Độ dài chuỗi (phải khớp với lúc train)
NUM_NEG_TEST = 99  # Sampled Metrics: 1 Positive vs 99 Negatives
SEED = 42

# Thiết lập Seed
np.random.seed(SEED)
tf.random.set_seed(SEED)

# =============================================================================
# 3. HÀM TẢI ARTIFACTS TỪ MLFLOW
# =============================================================================
def fetch_artifacts_and_get_run_id():
    print(f"\n🌍 Đang kết nối MLflow tại: {os.environ['MLFLOW_TRACKING_URI']}")
    mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])
    
    # 1. Tìm Experiment
    exp = mlflow.get_experiment_by_name(EXPERIMENT_NAME)
    if exp is None:
        raise ValueError(f"❌ Không tìm thấy Experiment tên: '{EXPERIMENT_NAME}'")
    
    # 2. Tìm Run mới nhất (đã hoàn thành)
    print("🔍 Đang tìm Run mới nhất có trạng thái 'FINISHED'...")
    runs = mlflow.search_runs(
        experiment_ids=[exp.experiment_id],
        filter_string="status = 'FINISHED'",
        order_by=["start_time DESC"],
        max_results=1
    )
    
    if runs.empty:
        raise ValueError("❌ Không tìm thấy Run nào đã hoàn thành (FINISHED).")
    
    last_run_id = runs.iloc[0].run_id
    print(f"✅ Đã tìm thấy Run ID: {last_run_id}")

    # 3. Tạo thư mục tạm
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    # 4. Tải Model & Data
    print("📥 Đang tải Model và Test Set từ MinIO...")
    
    # Lưu ý: artifact_path phải khớp với lúc bạn log trong code training
    # Mặc định là: 'model_keras' và 'data_splits'
    model_path = mlflow.artifacts.download_artifacts(run_id=last_run_id, artifact_path="model_keras/sasrec_v1.keras", dst_path=DOWNLOAD_DIR)
    test_set_path = mlflow.artifacts.download_artifacts(run_id=last_run_id, artifact_path="data_splits/test_set.pkl", dst_path=DOWNLOAD_DIR)
    
    # Thử tải item_map (nếu có)
    try:
        map_path = mlflow.artifacts.download_artifacts(run_id=last_run_id, artifact_path="metadata/item_map.json", dst_path=DOWNLOAD_DIR)
    except:
        print("⚠️ Cảnh báo: Không tìm thấy 'item_map.json' trên Server. Sẽ tự động suy luận Vocab Size.")
        map_path = None

    return last_run_id, model_path, test_set_path, map_path

# =============================================================================
# 4. HÀM XỬ LÝ DỮ LIỆU (PADDING)
# =============================================================================
def pad_sequence(seq, max_len):
    seq = list(seq)
    pad_len = max_len - len(seq)
    # Post-padding: Thêm số 0 vào sau đuôi
    padded_seq = seq + [0] * pad_len
    # Mask: True cho vị trí có giá trị, False cho padding
    mask = [True] * len(seq) + [False] * pad_len
    return np.array(padded_seq, dtype=np.int32), np.array(mask, dtype=bool)

# =============================================================================
# 5. MAIN EVALUATION LOOP
# =============================================================================
def main():
    print("\n" + "="*60)
    print("🚀 BẮT ĐẦU QUÁ TRÌNH ĐÁNH GIÁ (OFFLINE EVALUATION)")
    print("="*60)

    # --- BƯỚC 1: TẢI DATA ---
    try:
        run_id, model_path, test_set_path, item_map_path = fetch_artifacts_and_get_run_id()
    except Exception as e:
        print(f"❌ Lỗi tải Artifacts: {e}")
        return

    # --- BƯỚC 2: LOAD DATA & MODEL ---
    print(f"\n📂 Đang đọc dữ liệu...")
    with open(test_set_path, "rb") as f:
        test_set = pickle.load(f)
    
    # Xác định Vocab Size
    if item_map_path and os.path.exists(item_map_path):
        with open(item_map_path, "r") as f:
            item_map = json.load(f)
        vocab_size = len(item_map)
    else:
        # Fallback: Quét toàn bộ test set để tìm max ID
        all_items = [x for s in test_set for x in s['input_items']]
        vocab_size = max(all_items)
        print(f"⚠️ Đã suy luận Vocab Size từ dữ liệu test: {vocab_size}")

    print(f"🏗️ Đang load Model từ: {model_path}")
    try:
        # Load model nguyên khối, mapping class SasRec
        model = tf.keras.models.load_model(model_path, custom_objects={'SasRec': SasRec}, compile=False)
        print("✅ Model loaded thành công!")
    except Exception as e:
        print(f"❌ Lỗi load model Keras: {e}")
        return

    # --- BƯỚC 3: CHẠY ĐÁNH GIÁ ---
    hits_10 = 0
    ndcgs_10 = 0
    
    # Cache Embedding Matrix để tăng tốc
    item_weights = model.item_embedding.embeddings 

    print(f"\n🏁 Đang chạy Test trên {len(test_set)} users (Sampled 1 vs {NUM_NEG_TEST} Negatives)...")
    
    for sample in tqdm(test_set, desc="Evaluating"):
        seq_items = sample["input_items"]
        seq_cats = sample["input_cats"]
        target_item = sample["label"]

        # A. Negative Sampling (Chọn 99 item ngẫu nhiên user chưa xem)
        seen_items = set(seq_items)
        seen_items.add(target_item)
        
        # List candidate gồm 1 Positive đầu tiên + 99 Negatives
        test_items = [target_item]
        while len(test_items) < NUM_NEG_TEST + 1:
            neg = np.random.randint(1, vocab_size + 1)
            if neg not in seen_items:
                test_items.append(neg)

        # B. Tiền xử lý Input (Cắt & Padding)
        # Chỉ lấy MAX_LEN item cuối cùng
        seq_items_sliced = list(seq_items)[-MAX_LEN:]
        seq_cats_sliced = list(seq_cats)[-MAX_LEN:]
        valid_len = len(seq_items_sliced)

        in_items, mask = pad_sequence(seq_items_sliced, MAX_LEN)
        in_cats, _ = pad_sequence(seq_cats_sliced, MAX_LEN)

        # Thêm Batch Dimension [1, MAX_LEN]
        in_items = np.expand_dims(in_items, axis=0)
        in_cats = np.expand_dims(in_cats, axis=0)
        mask = np.expand_dims(mask, axis=0)

        # C. Inference (Dự đoán)
        outputs = model(
            {"item_ids": in_items, "category_ids": in_cats, "padding_mask": mask},
            training=False
        )
        
        # Lấy vector đại diện User tại thời điểm cuối cùng (không lấy padding)
        # Sequence Embedding shape: [1, MAX_LEN, Embed_Dim]
        seq_emb = outputs["item_sequence_embedding"][0]
        user_emb = seq_emb[valid_len - 1] 

        # D. Ranking
        # Lấy vector của 100 item candidates
        test_items_idx = tf.constant(test_items, dtype=tf.int32)
        test_item_embs = tf.gather(item_weights, test_items_idx)

        # Tính Dot Product: (100, D) . (D, 1) -> (100,)
        scores = tf.matmul(test_item_embs, tf.expand_dims(user_emb, -1))
        scores = tf.squeeze(scores).numpy()

        # Item đầu tiên (index 0) là Ground Truth
        # Rank = Số lượng item có điểm cao hơn Ground Truth
        rank = np.sum(scores > scores[0])

        # E. Tính Metrics
        if rank < 10:
            hits_10 += 1
            ndcgs_10 += 1.0 / np.log2(rank + 2)

    # --- BƯỚC 4: TỔNG HỢP KẾT QUẢ ---
    hr_10 = hits_10 / len(test_set)
    ndcg_10 = ndcgs_10 / len(test_set)

    print("\n" + "=" * 50)
    print(f"🏆 KẾT QUẢ ĐÁNH GIÁ (Run ID: {run_id})")
    print("=" * 50)
    print(f"🎯 Hit Rate @10 : {hr_10:.4f}")
    print(f"⭐ NDCG @10     : {ndcg_10:.4f}")
    print("=" * 50)

    # --- BƯỚC 5: LOG KẾT QUẢ NGƯỢC VÀO MLFLOW ---
    print(f"\n📝 Đang ghi metrics vào MLflow Run: {run_id}...")
    try:
        # Sử dụng đúng Run ID đã tải về lúc nãy để ghi đè metrics
        with mlflow.start_run(run_id=run_id):
            mlflow.log_metrics({
                "test_hit_rate_10": hr_10,
                "test_ndcg_10": ndcg_10
            })
        print("✅ Thành công! Bạn có thể kiểm tra trên MLflow UI.")
    except Exception as e:
        print(f"⚠️ Lỗi khi log metrics vào MLflow: {e}")

if __name__ == "__main__":
    main()