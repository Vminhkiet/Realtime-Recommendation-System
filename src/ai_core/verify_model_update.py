import os
import json
import pickle
import numpy as np
import tensorflow as tf
import mlflow
import psycopg2
from collections import Counter
from datetime import datetime
from tqdm import tqdm

# =============================================================================
# 1. SETUP & CONFIG
# =============================================================================
os.environ["AWS_ACCESS_KEY_ID"] = "minioadmin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "minioadmin"
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
os.environ["MLFLOW_S3_ENDPOINT_URL"] = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
os.environ["MLFLOW_TRACKING_URI"] = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")

DB_CONF = {
    "host": "timescaledb", 
    "port": "5432",
    "user": "postgres",
    "password": "password",
    "dbname": "ecommerce_logs"
}

EXPERIMENT_NAME = "sasrec_original_config_kiet"
DOWNLOAD_DIR = "./downloaded_artifacts"
MAX_LEN = 50
NUM_NEG_TEST = 99
SEED = 42

np.random.seed(SEED)
tf.random.set_seed(SEED)

# =============================================================================
# 2. MODEL & UTILS
# =============================================================================
try:
    from model import SasRec
except ImportError:
    try:
        from src.ai_core.model import SasRec
    except ImportError:
        print("❌ LỖI: Không tìm thấy class 'SasRec'.")
        import sys; sys.exit(1)

def fetch_artifacts_and_get_run_id():
    # (Giữ nguyên code tải artifact như các bài trước)
    print(f"\n🌍 Đang kết nối MLflow...")
    mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])
    exp = mlflow.get_experiment_by_name(EXPERIMENT_NAME)
    if not exp: raise ValueError("Experiment not found")
    
    runs = mlflow.search_runs(experiment_ids=[exp.experiment_id], filter_string="status = 'FINISHED'", order_by=["start_time DESC"], max_results=1)
    if runs.empty: raise ValueError("No run found")
    
    last_run_id = runs.iloc[0].run_id
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    
    model_path = mlflow.artifacts.download_artifacts(run_id=last_run_id, artifact_path="model_keras/sasrec_v1.keras", dst_path=DOWNLOAD_DIR)
    test_set_path = mlflow.artifacts.download_artifacts(run_id=last_run_id, artifact_path="data_splits/test_set.pkl", dst_path=DOWNLOAD_DIR)
    try: map_path = mlflow.artifacts.download_artifacts(run_id=last_run_id, artifact_path="metadata/item_map.json", dst_path=DOWNLOAD_DIR)
    except: map_path = None
    
    return last_run_id, model_path, test_set_path, map_path

def pad_sequence(seq, max_len):
    seq = list(seq)
    pad_len = max_len - len(seq)
    padded_seq = seq + [0] * pad_len
    mask = [True] * len(seq) + [False] * pad_len
    return np.array(padded_seq, dtype=np.int32), np.array(mask, dtype=bool)

def save_benchmark_to_db(results, run_id):
    """Lưu kết quả so sánh 3 thuật toán vào DB"""
    print(f"\n🗄️ Đang lưu Benchmark vào Database...")
    now = datetime.now()
    
    # Payload JSON chứa kết quả so sánh
    metrics_payload = {
        "evaluated_at": now.isoformat(),
        "mlflow_run_id": run_id,
        "valid_samples": results["count"],
        
        # 1. Random
        "random_hr_10": round(results["Random"]["hr"], 4),
        "random_ndcg_10": round(results["Random"]["ndcg"], 4),
        
        # 2. Most Popular
        "pop_hr_10": round(results["MostPopular"]["hr"], 4),
        "pop_ndcg_10": round(results["MostPopular"]["ndcg"], 4),
        
        # 3. SasRec (Main)
        "sasrec_hr_10": round(results["SasRec"]["hr"], 4),
        "sasrec_ndcg_10": round(results["SasRec"]["ndcg"], 4),
        
        # 4. Fields chuẩn để hiện trên Dashboard cũ (dùng SasRec làm đại diện)
        "hit_rate_10": round(results["SasRec"]["hr"], 4),
        "ndcg_10": round(results["SasRec"]["ndcg"], 4)
    }

    try:
        conn = psycopg2.connect(**DB_CONF)
        cur = conn.cursor()
        cur.execute("""
            UPDATE model_registry 
            SET metrics = %s, status = 'EVALUATED'
            WHERE model_id = 'sasrec-video-games' OR model_path LIKE %s
        """, (json.dumps(metrics_payload), f"%{run_id}%"))
        conn.commit()
        print(f"✅ [DB] Đã lưu bảng so sánh Benchmark vào DB.")
    except Exception as e:
        print(f"❌ [DB] Lỗi: {e}")
    finally:
        if 'conn' in locals() and conn: conn.close()

# =============================================================================
# 3. LOGIC TÍNH TOÁN BASELINE
# =============================================================================
def calculate_popularity_scores(test_set):
    """Tính độ phổ biến của item dựa trên dữ liệu test (hoặc train nếu có)"""
    print("📊 Đang tính toán độ phổ biến (Most Popular)...")
    all_items = []
    for s in test_set:
        all_items.extend(s['input_items'])
    
    # Đếm tần suất xuất hiện
    item_counts = Counter(all_items)
    return item_counts

# =============================================================================
# 4. MAIN PROGRAM
# =============================================================================
def main():
    print("\n" + "="*60)
    print("🚀 BENCHMARK: SASREC vs RANDOM vs MOST POPULAR")
    print("="*60)

    # 1. Load Data
    try:
        run_id, model_path, test_set_path, item_map_path = fetch_artifacts_and_get_run_id()
    except Exception as e:
        print(f"❌ {e}"); return

    with open(test_set_path, "rb") as f: test_set = pickle.load(f)
    
    # Load Vocab Size
    if item_map_path and os.path.exists(item_map_path):
        with open(item_map_path, "r") as f: vocab_size = len(json.load(f))
    else:
        all_items = [x for s in test_set for x in s['input_items']]
        vocab_size = max(all_items)

    # 2. Chuẩn bị Baseline
    # Tính Popularity Map (Item ID -> Số lần xuất hiện)
    pop_map = calculate_popularity_scores(test_set)

    # 3. Load Model SasRec
    print(f"🏗️ Loading SasRec Model...")
    model = tf.keras.models.load_model(model_path, custom_objects={'SasRec': SasRec}, compile=False)
    item_weights = model.item_embedding.embeddings 

    # 4. EVALUATION LOOP
    metrics = {
        "Random": {"hits": 0, "ndcg": 0},
        "MostPopular": {"hits": 0, "ndcg": 0},
        "SasRec": {"hits": 0, "ndcg": 0}
    }
    
    print(f"\n🏁 Đang chạy đua 3 thuật toán trên {len(test_set)} users...")
    
    for sample in tqdm(test_set):
        seq_items = sample["input_items"]
        seq_cats = sample["input_cats"]
        target_item = sample["label"]

        # --- A. Common Setup (Negative Sampling) ---
        seen_items = set(seq_items)
        seen_items.add(target_item)
        
        # Tạo danh sách 100 ứng viên (1 thật + 99 giả)
        candidates = [target_item]
        while len(candidates) < NUM_NEG_TEST + 1:
            neg = np.random.randint(1, vocab_size + 1)
            if neg not in seen_items:
                candidates.append(neg)
        
        # --- B. CHẤM ĐIỂM TỪNG THUẬT TOÁN ---
        
        # 1. RANDOM (Gán điểm ngẫu nhiên)
        rand_scores = np.random.rand(len(candidates))
        
        # 2. MOST POPULAR (Gán điểm bằng số lần xuất hiện)
        # Nếu item chưa từng thấy thì score = 0
        pop_scores = np.array([pop_map.get(i, 0) for i in candidates])
        # Cộng thêm nhiễu cực nhỏ để phá vỡ thế cân bằng (break ties)
        pop_scores = pop_scores + np.random.uniform(0, 1e-6, size=len(pop_scores))

        # 3. SASREC (Deep Learning)
        # Preprocessing
        seq_i = list(seq_items)[-MAX_LEN:]
        seq_c = list(seq_cats)[-MAX_LEN:]
        valid_len = len(seq_i)
        in_i, mask = pad_sequence(seq_i, MAX_LEN)
        in_c, _ = pad_sequence(seq_c, MAX_LEN)
        
        outputs = model({
            "item_ids": np.expand_dims(in_i, 0), 
            "category_ids": np.expand_dims(in_c, 0), 
            "padding_mask": np.expand_dims(mask, 0)
        }, training=False)
        
        user_emb = outputs["item_sequence_embedding"][0][valid_len - 1]
        
        cand_tensor = tf.constant(candidates, dtype=tf.int32)
        cand_embs = tf.gather(item_weights, cand_tensor)
        sasrec_scores = tf.matmul(cand_embs, tf.expand_dims(user_emb, -1))
        sasrec_scores = tf.squeeze(sasrec_scores).numpy()

        # --- C. RANKING & METRICS ---
        # Hàm tính Rank chung cho cả 3
        def calculate_rank(scores):
            # Điểm của món đồ thật nằm ở index 0
            target_score = scores[0]
            # Rank = số lượng món có điểm cao hơn món thật
            return np.sum(scores > target_score)

        # Tính rank cho từng thuật toán
        rank_rand = calculate_rank(rand_scores)
        rank_pop = calculate_rank(pop_scores)
        rank_sas = calculate_rank(sasrec_scores)

        # Cộng điểm
        for alg, r in zip(["Random", "MostPopular", "SasRec"], [rank_rand, rank_pop, rank_sas]):
            if r < 10:
                metrics[alg]["hits"] += 1
                metrics[alg]["ndcg"] += 1.0 / np.log2(r + 2)

    # 5. TỔNG HỢP KẾT QUẢ
    final_results = {"count": len(test_set)}
    
    print("\n" + "=" * 60)
    print(f"🏆 BẢNG XẾP HẠNG CUỐI CÙNG (Benchmark)")
    print("=" * 60)
    print(f"{'THUẬT TOÁN':<15} | {'HR@10':<10} | {'NDCG@10':<10} | {'NHẬN XÉT'}")
    print("-" * 60)

    for alg in ["Random", "MostPopular", "SasRec"]:
        hr = metrics[alg]["hits"] / len(test_set)
        ndcg = metrics[alg]["ndcg"] / len(test_set)
        final_results[alg] = {"hr": hr, "ndcg": ndcg}
        
        comment = "Tệ (Đoán mò)" if alg == "Random" else ("Khá (Theo Trend)" if alg == "MostPopular" else "TỐT NHẤT 🚀")
        print(f"{alg:<15} | {hr:.4f}     | {ndcg:.4f}     | {comment}")

    print("=" * 60)

    # 6. LƯU VÀO DB
    save_benchmark_to_db(final_results, run_id)

if __name__ == "__main__":
    main()