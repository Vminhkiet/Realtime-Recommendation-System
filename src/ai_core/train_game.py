# import os
# import json
# import random
# import pickle
# import pandas as pd
# import tensorflow as tf
# from collections import Counter
# import numpy as np

# # Cố gắng import class SasRec
# try:
#     from model import SasRec
# except ImportError:
#     from src.ai_core.model import SasRec

# # ======================================================
# # 1. CẤU HÌNH & PATHS
# # ======================================================
# BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# PROJECT_ROOT = os.path.dirname(os.path.dirname(BASE_DIR))

# # Input Paths
# PARQUET_PATH = os.path.join(PROJECT_ROOT, 'data/model_registry/processed_parquet')
# MAP_PATH = os.path.join(PROJECT_ROOT, 'data/model_registry/item_map.json')
# CAT_MAP_PATH = os.path.join(PROJECT_ROOT, 'data/model_registry/category_map.json')

# # Output Paths
# MODEL_SAVE_PATH = os.path.join(PROJECT_ROOT, 'data/model_registry/sasrec_v1.keras')
# TEST_SET_SAVE_PATH = os.path.join(PROJECT_ROOT, 'data/model_registry/test_set.pkl')
# LOG_PATH = os.path.join(PROJECT_ROOT, 'data/training_history.json')
# VALID_USERS_PATH = os.path.join(PROJECT_ROOT, 'src/simulation/valid_users.json')

# # ======================================================
# # 2. HYPERPARAMETERS
# # ======================================================
# MAX_USERS = 10000       # Lấy 10k user tốt nhất để train
# MAX_LEN = 50            # Độ dài chuỗi tối đa

# # Model Config
# EMBED_DIM = 64
# NUM_BLOCKS = 2
# NUM_HEADS = 4
# DROPOUT_RATE = 0.1

# # Training Config
# BATCH_SIZE = 64
# EPOCHS = 50
# STEPS_PER_EPOCH = 200   # Số bước trong 1 epoch
# STEP_SIZE = 10          # Sliding Window nhảy cóc (để tạo nhiều mẫu train)

# # ======================================================
# # 3. UTILS
# # ======================================================
# def normalize_ts(ts):
#     # Chuẩn hóa timestamp về giây nếu đang ở milisecond
#     return ts / 1000 if ts > 32503680000 else ts

# def get_popular_items(sequences, top_k=2000):
#     print("🔥 Đang tính Popular Items (để làm Hard Negatives)...")
#     all_items = [i for seq in sequences for i in seq]
#     counter = Counter(all_items)
#     # Bỏ qua padding (0)
#     if 0 in counter: del counter[0]
#     return [item for item, _ in counter.most_common(top_k)]

# # ======================================================
# # 4. LOAD DATA (FIX LỖI TIMESTAMP)
# # ======================================================
# def load_data():
#     print("📥 Đang load dữ liệu Parquet...")
#     if not os.path.exists(PARQUET_PATH):
#         raise FileNotFoundError(f"❌ Không tìm thấy file: {PARQUET_PATH}")

#     df = pd.read_parquet(PARQUET_PATH)
    
#     # --- Xử lý cột thời gian (Fix lỗi KeyError) ---
#     if 'last_timestamp' in df.columns:
#         TIME_COL = 'last_timestamp'
#     elif 'sequence_timestamps' in df.columns:
#         # Lấy phần tử cuối cùng trong list timestamp
#         df['last_timestamp'] = df['sequence_timestamps'].apply(lambda x: x[-1] if len(x) > 0 else 0)
#         TIME_COL = 'last_timestamp'
#     else:
#         df['last_timestamp'] = 0
#         TIME_COL = 'last_timestamp'

#     df[TIME_COL] = df[TIME_COL].apply(normalize_ts)

#     # Đảm bảo có user_id
#     if 'user_id' not in df.columns:
#         df['user_id'] = df.index.astype(str)

#     # --- Lọc User chất lượng nhất ---
#     print(f"🔍 Sàng lọc Top {MAX_USERS} Users có lịch sử dày nhất...")
#     df['seq_len'] = df['sequence_ids'].apply(len)
    
#     # Sắp xếp giảm dần theo độ dài lịch sử & lấy Top K
#     df_sorted = df.sort_values(by='seq_len', ascending=False).head(MAX_USERS)
    
#     # Shuffle ngẫu nhiên để train khách quan
#     df_final = df_sorted.sample(frac=1).reset_index(drop=True)

#     print(f"✅ Đã chọn: {len(df_final)} users.")

#     # Export ra list
#     item_seqs = df_final['sequence_ids'].tolist()
#     cat_seqs = df_final['category_ids'].tolist()
#     last_times = df_final[TIME_COL].tolist()
#     valid_user_ids = df_final['user_id'].tolist()

#     # Lưu danh sách user để dùng cho Simulation sau này
#     os.makedirs(os.path.dirname(VALID_USERS_PATH), exist_ok=True)
#     with open(VALID_USERS_PATH, 'w') as f:
#         json.dump(valid_user_ids, f)

#     # Load Metadata Maps
#     with open(MAP_PATH, 'r') as f:
#         vocab_size = len(json.load(f))
    
#     if os.path.exists(CAT_MAP_PATH):
#         with open(CAT_MAP_PATH, 'r') as f:
#             num_categories = len(json.load(f))
#     else:
#         num_categories = 100

#     return item_seqs, cat_seqs, last_times, vocab_size, num_categories

# # ======================================================
# # 5. DATASET GENERATOR (SLIDING WINDOW)
# # ======================================================
# def create_dataset(item_seqs, cat_seqs, max_len, num_items, popular_items, is_training=True):
#     def generator():
#         # Zip và Shuffle nếu là Train
#         data = list(zip(item_seqs, cat_seqs))
#         if is_training:
#             random.shuffle(data)
        
#         for item_seq, cat_seq in data:
#             item_seq = [int(x) for x in item_seq]
#             cat_seq = [int(x) for x in cat_seq]
#             seq_len = len(item_seq)

#             # --- LOGIC SLIDING WINDOW ---
#             if is_training:
#                 # TRAIN: Cắt nhiều đoạn nhỏ để học (Sliding)
#                 # VD: [A, B, C, D] -> [A, B], [A, B, C], [A, B, C, D]
#                 if seq_len <= max_len + 1:
#                     starts = [0]
#                 else:
#                     starts = range(0, seq_len - max_len, STEP_SIZE)
#             else:
#                 # VALIDATION: KHÔNG SLIDING.
#                 # Chỉ lấy đúng 1 đoạn cuối cùng để kiểm tra khả năng dự đoán tương lai.
#                 starts = [max(0, seq_len - max_len - 1)]

#             for i in starts:
#                 # Cắt chuỗi
#                 end = min(i + max_len + 1, seq_len)
#                 item_win = item_seq[i:end]
#                 cat_win = cat_seq[i:end]
                
#                 if len(item_win) < 2: continue

#                 # Tách Input và Target
#                 # Input:  [A, B, C]
#                 # Target: [B, C, D]
#                 curr_item = item_win[:-1]
#                 curr_pos = item_win[1:]
#                 curr_cat = cat_win[:-1]

#                 # Cắt đúng MAX_LEN
#                 curr_item = curr_item[-max_len:]
#                 curr_pos = curr_pos[-max_len:]
#                 curr_cat = curr_cat[-max_len:]

#                 # Padding (thêm số 0 vào sau)
#                 pad_len = max_len - len(curr_item)
#                 input_ids = curr_item + [0] * pad_len
#                 pos_ids = curr_pos + [0] * pad_len
#                 cat_ids = curr_cat + [0] * pad_len
                
#                 # Mask (True = Dữ liệu thật, False = Padding)
#                 mask = [True] * len(curr_item) + [False] * pad_len

#                 # Negative Sampling (Chọn item sai để model phân biệt)
#                 win_set = set(item_win)
#                 neg_ids = []
#                 for _ in range(len(curr_item)):
#                     # 30% chọn Popular Item (Khó), 70% chọn Random (Dễ)
#                     if is_training and random.random() < 0.3 and popular_items:
#                         neg = random.choice(popular_items)
#                     else:
#                         neg = random.randint(1, num_items)
                    
#                     # Đảm bảo item âm không trùng item thật
#                     while neg in win_set:
#                         neg = random.randint(1, num_items)
#                     neg_ids.append(neg)
                
#                 neg_ids += [0] * pad_len

#                 yield (
#                     {"item_ids": input_ids, "category_ids": cat_ids, "padding_mask": mask},
#                     {"positive_sequence": pos_ids, "negative_sequence": neg_ids}
#                 )

#     return tf.data.Dataset.from_generator(
#         generator,
#         output_signature=(
#             {
#                 "item_ids": tf.TensorSpec((max_len,), tf.int32),
#                 "category_ids": tf.TensorSpec((max_len,), tf.int32),
#                 "padding_mask": tf.TensorSpec((max_len,), tf.bool),
#             },
#             {
#                 "positive_sequence": tf.TensorSpec((max_len,), tf.int32),
#                 "negative_sequence": tf.TensorSpec((max_len,), tf.int32),
#             }
#         )
#     ).batch(BATCH_SIZE).prefetch(tf.data.AUTOTUNE)

# # ======================================================
# # 6. MAIN FUNCTION
# # ======================================================
# def main():
#     print("🚀 TRAINING START: STRATEGY (TRAIN - VAL - TEST)")
    
#     # 1. Load Data
#     item_seqs, cat_seqs, last_times, vocab_size, num_categories = load_data()
#     popular_items = get_popular_items(item_seqs)

#     # 2. SPLIT DATA (QUAN TRỌNG)
#     # Chiến lược: Giữ lại 2 item cuối. 
#     # - 1 cái cho Test (để báo cáo).
#     # - 1 cái cho Validation (để chỉnh model).
    
#     train_items, train_cats = [], []
#     val_items, val_cats = [], []
#     test_set = []

#     print("✂️ Splitting: Train (History) | Val (Last-1) | Test (Last)")
    
#     for seq, cat, ts in zip(item_seqs, cat_seqs, last_times):
#         # Cần ít nhất 3 món để chia (1 train, 1 val, 1 test)
#         if len(seq) < 3: continue 

#         # --- A. TEST SET (Món cuối cùng) ---
#         test_set.append({
#             "input_items": seq[:-1],    # Dùng lịch sử trước đó để đoán món cuối
#             "input_cats": cat[:-1],
#             "label": seq[-1],           # Đáp án là món cuối
#             "test_time": ts
#         })

#         # --- B. VALIDATION SET (Món kế cuối) ---
#         # Ta đưa chuỗi seq[:-1] vào val generator.
#         # Generator (với is_training=False) sẽ lấy cửa sổ cuối cùng.
#         # Tức là Input [..., N-3, N-2] -> Target [..., N-2, N-1]
#         # Như vậy model được kiểm tra trên món N-1 (Món kế cuối).
#         val_items.append(seq[:-1])
#         val_cats.append(cat[:-1])

#         # --- C. TRAIN SET (Phần còn lại) ---
#         # Train chỉ được nhìn thấy đến món N-2
#         train_items.append(seq[:-2])
#         train_cats.append(cat[:-2])

#     print(f"📊 Stats: {len(train_items)} Train | {len(val_items)} Val | {len(test_set)} Test users")

#     # Lưu tập Test ra file để sau này chạy eval_game.py
#     with open(TEST_SET_SAVE_PATH, 'wb') as f:
#         pickle.dump(test_set, f)

#     # 3. Tạo Datasets
#     # Train có Sliding Window, Val thì không
#     train_ds = create_dataset(train_items, train_cats, MAX_LEN, vocab_size, popular_items, is_training=True)
#     val_ds = create_dataset(val_items, val_cats, MAX_LEN, vocab_size, popular_items, is_training=False)

#     # 4. Build Model
#     print("🏗️ Building SASRec Model...")
#     model = SasRec(
#         vocabulary_size=vocab_size + 1,
#         category_size=num_categories + 1,
#         num_layers=NUM_BLOCKS,
#         num_heads=NUM_HEADS,
#         hidden_dim=EMBED_DIM,
#         dropout=DROPOUT_RATE,
#         max_sequence_length=MAX_LEN
#     )

#     # Learning Rate Schedule (Cosine Decay)
#     lr_schedule = tf.keras.optimizers.schedules.CosineDecayRestarts(
#         initial_learning_rate=0.001,
#         first_decay_steps=STEPS_PER_EPOCH * 5,
#         t_mul=2.0, m_mul=0.9, alpha=1e-6
#     )
    
#     model.compile(optimizer=tf.keras.optimizers.AdamW(learning_rate=lr_schedule, weight_decay=0.01))

#     # Callbacks (Monitor trên VAL_LOSS)
#     callbacks = [
#         tf.keras.callbacks.ModelCheckpoint(
#             MODEL_SAVE_PATH,
#             save_best_only=True,
#             monitor='val_loss', # Quan trọng: Monitor Val Loss để lưu model tổng quát nhất
#             mode='min',
#             save_freq='epoch',
#             verbose=1
#         ),
#         tf.keras.callbacks.EarlyStopping(
#             patience=5,             # Dừng nếu 5 epoch liên tiếp không cải thiện
#             restore_best_weights=True, 
#             monitor='val_loss'
#         )
#     ]

#     # 5. Start Training
#     print(f"🔥 Bắt đầu Training...")
#     history = model.fit(
#         train_ds,
#         validation_data=val_ds,
#         epochs=EPOCHS,
#         steps_per_epoch=STEPS_PER_EPOCH,
#         validation_steps=50, # Kiểm tra 50 batch của tập val mỗi epoch
#         callbacks=callbacks
#     )

#     # 6. Save Log
#     try:
#         with open(LOG_PATH, 'w') as f:
#             json.dump(history.history, f)
#         print("📊 Đã lưu Training Log.")
#     except Exception as e:
#         print(f"⚠️ Không lưu được log: {e}")

# if __name__ == "__main__":
#     main()


import os
import sys

# =============================================================================
# 1. SETUP CREDENTIALS (THÊM ĐOẠN NÀY ĐỂ FIX LỖI S3)
# =============================================================================
os.environ["AWS_ACCESS_KEY_ID"] = "minioadmin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "minioadmin"
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

# Endpoint cho MinIO Local
MINIO_URL = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
os.environ["MLFLOW_S3_ENDPOINT_URL"] = MINIO_URL
os.environ["AWS_ENDPOINT_URL"] = MINIO_URL
os.environ["AWS_S3_ENDPOINT_URL"] = MINIO_URL
os.environ["MLFLOW_TRACKING_URI"] = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")

# =============================================================================
# 2. IMPORTS
# =============================================================================
import json
import random
import pickle
import pandas as pd
import tensorflow as tf
from collections import Counter
import numpy as np
import mlflow            # <--- Mới
import mlflow.tensorflow # <--- Mới
import boto3             # <--- Mới

# Cố gắng import class SasRec
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from model import SasRec
except ImportError:
    try:
        from src.ai_core.model import SasRec
    except ImportError:
        print("❌ CRITICAL: Cannot import SasRec model class.")
        sys.exit(1)

# ======================================================
# 1. CẤU HÌNH & PATHS
# ======================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(BASE_DIR))

# Input Paths
PARQUET_PATH = os.path.join(PROJECT_ROOT, 'data/model_registry/processed_parquet')
MAP_PATH = os.path.join(PROJECT_ROOT, 'data/model_registry/item_map.json')
CAT_MAP_PATH = os.path.join(PROJECT_ROOT, 'data/model_registry/category_map.json')

# Output Paths (Sửa lại lưu vào /tmp để upload lên MLflow)
MODEL_SAVE_PATH = "data/model_registry/sasrec_v1.keras"
TEST_SET_SAVE_PATH = "data/model_registry/test_set.pkl"
LOG_PATH = "data/model_registry/training_history.json"
VALID_USERS_PATH = os.path.join(PROJECT_ROOT, 'src/simulation/valid_users.json')

# ======================================================
# 2. HYPERPARAMETERS (GIỮ NGUYÊN CỦA ÔNG)
# ======================================================
MAX_USERS = 10000       # Lấy 10k user tốt nhất để train
MAX_LEN = 50            # Độ dài chuỗi tối đa

# Model Config
EMBED_DIM = 64
NUM_BLOCKS = 2
NUM_HEADS = 4
DROPOUT_RATE = 0.1

# Training Config
BATCH_SIZE = 64
EPOCHS = 50
STEPS_PER_EPOCH = 200   # Số bước trong 1 epoch
STEP_SIZE = 10          # Sliding Window nhảy cóc

# ======================================================
# 3. UTILS
# ======================================================
def ensure_bucket_exists(bucket_name="mlflow"):
    s3 = boto3.client("s3", endpoint_url=MINIO_URL)
    try: s3.head_bucket(Bucket=bucket_name)
    except: 
        try: s3.create_bucket(Bucket=bucket_name)
        except: pass

def normalize_ts(ts):
    return ts / 1000 if ts > 32503680000 else ts

def get_popular_items(sequences, top_k=2000):
    print("🔥 Đang tính Popular Items (để làm Hard Negatives)...")
    all_items = [i for seq in sequences for i in seq]
    counter = Counter(all_items)
    if 0 in counter: del counter[0]
    return [item for item, _ in counter.most_common(top_k)]

# ======================================================
# 4. LOAD DATA
# ======================================================
def load_data():
    print("📥 Đang load dữ liệu Parquet...")
    if not os.path.exists(PARQUET_PATH):
        raise FileNotFoundError(f"❌ Không tìm thấy file: {PARQUET_PATH}")

    df = pd.read_parquet(PARQUET_PATH)
    
    if 'last_timestamp' in df.columns:
        TIME_COL = 'last_timestamp'
    elif 'sequence_timestamps' in df.columns:
        df['last_timestamp'] = df['sequence_timestamps'].apply(lambda x: x[-1] if len(x) > 0 else 0)
        TIME_COL = 'last_timestamp'
    else:
        df['last_timestamp'] = 0
        TIME_COL = 'last_timestamp'

    df[TIME_COL] = df[TIME_COL].apply(normalize_ts)

    if 'user_id' not in df.columns:
        df['user_id'] = df.index.astype(str)

    print(f"🔍 Sàng lọc Top {MAX_USERS} Users có lịch sử dày nhất...")
    df['seq_len'] = df['sequence_ids'].apply(len)
    
    df_sorted = df.sort_values(by='seq_len', ascending=False).head(MAX_USERS)
    df_final = df_sorted.sample(frac=1).reset_index(drop=True)

    print(f"✅ Đã chọn: {len(df_final)} users.")

    item_seqs = df_final['sequence_ids'].tolist()
    cat_seqs = df_final['category_ids'].tolist()
    last_times = df_final[TIME_COL].tolist()
    valid_user_ids = df_final['user_id'].tolist()

    os.makedirs(os.path.dirname(VALID_USERS_PATH), exist_ok=True)
    with open(VALID_USERS_PATH, 'w') as f:
        json.dump(valid_user_ids, f)

    with open(MAP_PATH, 'r') as f:
        vocab_size = len(json.load(f))
    
    if os.path.exists(CAT_MAP_PATH):
        with open(CAT_MAP_PATH, 'r') as f:
            num_categories = len(json.load(f))
    else:
        num_categories = 100

    return item_seqs, cat_seqs, last_times, vocab_size, num_categories

# ======================================================
# 5. DATASET GENERATOR
# ======================================================
def create_dataset(item_seqs, cat_seqs, max_len, num_items, popular_items, is_training=True):
    def generator():
        data = list(zip(item_seqs, cat_seqs))
        if is_training:
            random.shuffle(data)
        
        for item_seq, cat_seq in data:
            # Ép kiểu list ngay từ đầu
            item_seq = list(item_seq)
            cat_seq = list(cat_seq)
            seq_len = len(item_seq)

            if is_training:
                if seq_len <= max_len + 1:
                    starts = [0]
                else:
                    starts = range(0, seq_len - max_len, STEP_SIZE)
            else:
                starts = [max(0, seq_len - max_len - 1)]

            for i in starts:
                end = min(i + max_len + 1, seq_len)
                item_win = item_seq[i:end]
                cat_win = cat_seq[i:end]
                
                if len(item_win) < 2: continue

                curr_item = item_win[:-1]
                curr_pos = item_win[1:]
                curr_cat = cat_win[:-1]

                curr_item = curr_item[-max_len:]
                curr_pos = curr_pos[-max_len:]
                curr_cat = curr_cat[-max_len:]

                # --- FIX LỖI CRASH Ở ĐÂY ---
                # Ép kiểu list() để cộng được với [0]*pad_len
                input_ids = list(curr_item)
                pos_ids = list(curr_pos)
                cat_ids = list(curr_cat)

                pad_len = max_len - len(input_ids)

                input_ids = input_ids + [0] * pad_len
                pos_ids = pos_ids + [0] * pad_len
                cat_ids = cat_ids + [0] * pad_len
                
                mask = [True] * len(curr_item) + [False] * pad_len

                win_set = set(item_win)
                neg_ids = []
                for _ in range(len(curr_item)):
                    if is_training and random.random() < 0.3 and popular_items:
                        neg = random.choice(popular_items)
                    else:
                        neg = random.randint(1, num_items)
                    
                    # Giữ nguyên logic check trùng của ông
                    while neg in win_set:
                        neg = random.randint(1, num_items)
                    neg_ids.append(neg)
                
                neg_ids += [0] * pad_len

                yield (
                    {"item_ids": input_ids, "category_ids": cat_ids, "padding_mask": mask},
                    {"positive_sequence": pos_ids, "negative_sequence": neg_ids}
                )

    # Thêm .repeat() để fix lỗi hết data giữa chừng (MLflow hay báo lỗi này)
    ds = tf.data.Dataset.from_generator(
        generator,
        output_signature=(
            {"item_ids": tf.TensorSpec((max_len,), tf.int32), "category_ids": tf.TensorSpec((max_len,), tf.int32), "padding_mask": tf.TensorSpec((max_len,), tf.bool)},
            {"positive_sequence": tf.TensorSpec((max_len,), tf.int32), "negative_sequence": tf.TensorSpec((max_len,), tf.int32)}
        )
    )

    if is_training:
        ds = ds.repeat().batch(BATCH_SIZE).prefetch(tf.data.AUTOTUNE)
    else:
        ds = ds.batch(BATCH_SIZE).prefetch(tf.data.AUTOTUNE)
    
    return ds

# ======================================================
# 6. MAIN FUNCTION
# ======================================================
def main():
    print("🚀 TRAINING START (MLflow Enabled)")
    ensure_bucket_exists("mlflow")

    # --- MLFLOW SETUP ---
    mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])
    mlflow.set_experiment("sasrec_original_config_kiet")
    mlflow.tensorflow.autolog(log_models=True, log_datasets=False)

    with mlflow.start_run() as run:
        # 1. Load Data
        item_seqs, cat_seqs, last_times, vocab_size, num_categories = load_data()
        popular_items = get_popular_items(item_seqs)

        # 2. SPLIT DATA
        train_items, train_cats = [], []
        val_items, val_cats = [], []
        test_set = []

        print("✂️ Splitting: Train | Val | Test")
        for seq, cat, ts in zip(item_seqs, cat_seqs, last_times):
            if len(seq) < 3: continue 
            test_set.append({"input_items": seq[:-1], "input_cats": cat[:-1], "label": seq[-1], "test_time": ts})
            val_items.append(seq[:-1])
            val_cats.append(cat[:-1])
            train_items.append(seq[:-2])
            train_cats.append(cat[:-2])

        print(f"📊 Stats: {len(train_items)} Train | {len(val_items)} Val | {len(test_set)} Test users")

        # 3. Create Datasets
        train_ds = create_dataset(train_items, train_cats, MAX_LEN, vocab_size, popular_items, is_training=True)
        val_ds = create_dataset(val_items, val_cats, MAX_LEN, vocab_size, popular_items, is_training=False)

        # 4. Log Params
        mlflow.log_params({
            "batch_size": BATCH_SIZE,
            "epochs": EPOCHS,
            "learning_rate_initial": 0.001,
            "steps_per_epoch": STEPS_PER_EPOCH
        })

        # 5. Build Model
        print("🏗️ Building SASRec Model...")
        model = SasRec(
            vocabulary_size=vocab_size + 1,
            category_size=num_categories + 1,
            num_layers=NUM_BLOCKS,
            num_heads=NUM_HEADS,
            hidden_dim=EMBED_DIM,
            dropout=DROPOUT_RATE,
            max_sequence_length=MAX_LEN
        )

        lr_schedule = tf.keras.optimizers.schedules.CosineDecayRestarts(
            initial_learning_rate=0.001,
            first_decay_steps=STEPS_PER_EPOCH * 5,
            t_mul=2.0, m_mul=0.9, alpha=1e-6
        )
        
        model.compile(optimizer=tf.keras.optimizers.AdamW(learning_rate=lr_schedule, weight_decay=0.01))

        # 6. Start Training
        print(f"🔥 Bắt đầu Training...")
        history = model.fit(
            train_ds,
            validation_data=val_ds,
            epochs=EPOCHS,
            steps_per_epoch=STEPS_PER_EPOCH,
            validation_steps=50,
            callbacks=[
                tf.keras.callbacks.EarlyStopping(monitor='val_loss', patience=5, restore_best_weights=True)
            ]
        )

        # 7. Save & Upload Artifacts
        print("💾 Saving artifacts...")
        with open(TEST_SET_SAVE_PATH, 'wb') as f:
            pickle.dump(test_set, f)
        model.save(MODEL_SAVE_PATH)

        try:
            with open(LOG_PATH, 'w') as f:
                json.dump(history.history, f)
            mlflow.log_artifact(LOG_PATH, artifact_path="logs")
        except: pass

        # Upload lên MLflow/MinIO
        try:
            mlflow.log_artifact(TEST_SET_SAVE_PATH, artifact_path="data_splits")
            mlflow.log_artifact(MODEL_SAVE_PATH, artifact_path="model_keras")
            if os.path.exists(MAP_PATH):
                mlflow.log_artifact(MAP_PATH, artifact_path="metadata")
            print("✅ Upload to MLflow Success!")
        except Exception as e:
            print(f"⚠️ Upload Failed: {e}")

if __name__ == "__main__":
    main()