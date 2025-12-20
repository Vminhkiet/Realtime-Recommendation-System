import os
import json
import random
import pickle
import pandas as pd
import tensorflow as tf
from collections import Counter
from datetime import datetime
from model import SasRec

# ======================================================
# CONFIG & PATHS
# ======================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(BASE_DIR)) # Giả sử file này ở src/model/

PARQUET_PATH = os.path.join(PROJECT_ROOT, 'data/model_registry/processed_parquet')
MAP_PATH = os.path.join(PROJECT_ROOT, 'data/model_registry/item_map.json')
CAT_MAP_PATH = os.path.join(PROJECT_ROOT, 'data/model_registry/category_map.json')
MODEL_SAVE_PATH = os.path.join(PROJECT_ROOT, 'data/model_registry/sasrec_v1.keras')
TEST_SET_SAVE_PATH = os.path.join(PROJECT_ROOT, 'data/model_registry/test_set.pkl')
LOG_PATH = os.path.join(PROJECT_ROOT, 'data/training_history.json')

# 🆕 ĐƯỜNG DẪN LƯU VALID USERS
VALID_USERS_PATH = os.path.join(PROJECT_ROOT, 'src/simulation/valid_users.json')

# ======================================================
# 🔥 HYPERPARAMETERS (HIGH PERFORMANCE MODE)
# ======================================================
MAX_USERS = 8000        
MAX_LEN = 50

# Cấu hình Model
EMBED_DIM = 64          
NUM_BLOCKS = 2          
NUM_HEADS = 4           
DROPOUT_RATE = 0.1      

# Cấu hình Training
BATCH_SIZE = 64         
EPOCHS = 50             
STEPS_PER_EPOCH = 400   
STEP_SIZE = 20          
MIN_INTERACTIONS = 5

# ======================================================
# UTILS
# ======================================================
def normalize_ts(ts):
    return ts / 1000 if ts > 32503680000 else ts

def get_popular_items(sequences, top_k=2000):
    print("🔥 Đang tính Popular Items (Hard Negatives)...")
    all_items = [i for seq in sequences for i in seq]
    counter = Counter(all_items)
    return [item for item, _ in counter.most_common(top_k)]

# ======================================================
# LOAD DATA & SAVE VALID USERS
# ======================================================
def load_data():
    print("📥 Đang load dữ liệu Parquet...")
    if not os.path.exists(PARQUET_PATH):
        raise FileNotFoundError("❌ Chưa chạy spark_process.py!")

    df = pd.read_parquet(PARQUET_PATH)
    TIME_COL = 'last_timestamp' 
    df[TIME_COL] = df[TIME_COL].apply(normalize_ts)

    # Lấy dữ liệu các cột
    item_seqs = df['sequence_ids'].tolist()
    cat_seqs = df['category_ids'].tolist()
    last_times = df[TIME_COL].tolist()
    
    # ⚠️ QUAN TRỌNG: Lấy cột user_id (đảm bảo trong parquet có cột tên là 'user_id')
    # Nếu cột tên khác (ví dụ 'user_str_id'), hãy sửa dòng dưới
    if 'user_id' in df.columns:
        user_ids = df['user_id'].tolist()
    else:
        # Fallback nếu không có cột user_id, dùng index
        print("⚠️ Không tìm thấy cột 'user_id', sử dụng index làm ID tạm.")
        user_ids = df.index.astype(str).tolist()

    # --- LỌC TOP USER TỐT NHẤT ---
    print(f"🔍 Sàng lọc {MAX_USERS} user tốt nhất từ {len(item_seqs)} user...")
    
    # Zip tất cả lại bao gồm cả user_id
    combined = list(zip(item_seqs, cat_seqs, last_times, user_ids))
    
    # Sort user có lịch sử dài nhất lên đầu
    combined.sort(key=lambda x: len(x[0]), reverse=True)
    
    # Cắt lấy Top MAX_USERS
    combined = combined[:MAX_USERS]
    
    # Unzip ra lại
    item_seqs, cat_seqs, last_times, valid_user_ids = zip(*combined)
    
    print(f"📉 Đã chọn: {len(item_seqs)} users.")

    # --- 💾 LƯU VALID USERS VÀO JSON ---
    print(f"💾 Đang lưu danh sách valid users vào: {VALID_USERS_PATH}")
    os.makedirs(os.path.dirname(VALID_USERS_PATH), exist_ok=True)
    with open(VALID_USERS_PATH, 'w') as f:
        json.dump(list(valid_user_ids), f) # Convert tuple to list for json

    # Load vocab size
    with open(MAP_PATH, 'r') as f:
        vocab_size = len(json.load(f))
    
    if os.path.exists(CAT_MAP_PATH):
        with open(CAT_MAP_PATH, 'r') as f:
            num_categories = len(json.load(f))
    else:
        num_categories = 100

    return item_seqs, cat_seqs, last_times, vocab_size, num_categories

# ======================================================
# DATASET GENERATOR
# ======================================================
def create_dataset(item_seqs, cat_seqs, max_len, num_items, popular_items):
    def generator():
        data = list(zip(item_seqs, cat_seqs))
        random.shuffle(data) 
        
        for item_seq, cat_seq in data:
            item_seq = list(item_seq)
            cat_seq = list(cat_seq)
            seq_len = len(item_seq)

            if seq_len <= max_len + 1:
                item_windows = [item_seq]
                cat_windows = [cat_seq]
            else:
                item_windows = [
                    item_seq[i:i + max_len + 1]
                    for i in range(0, seq_len - max_len, STEP_SIZE)
                ]
                cat_windows = [
                    cat_seq[i:i + max_len + 1]
                    for i in range(0, seq_len - max_len, STEP_SIZE)
                ]

            for item_win, cat_win in zip(item_windows, cat_windows):
                if len(item_win) < 2: continue

                curr_item = list(item_win[:-1])
                curr_pos = list(item_win[1:])
                curr_cat = list(cat_win[:-1])

                curr_item = curr_item[-max_len:]
                curr_pos = curr_pos[-max_len:]
                curr_cat = curr_cat[-max_len:]

                if random.random() < 0.1:
                    curr_item[random.randint(0, len(curr_item) - 1)] = 0

                pad_len = max_len - len(curr_item)

                input_ids = curr_item + [0] * pad_len
                pos_ids = curr_pos + [0] * pad_len
                cat_ids = curr_cat + [0] * pad_len
                mask = [True] * len(curr_item) + [False] * pad_len

                win_set = set(item_win)
                neg_ids = []
                for _ in range(len(curr_item)):
                    neg = random.choice(popular_items) if random.random() < 0.5 else random.randint(1, num_items)
                    while neg in win_set:
                        neg = random.randint(1, num_items)
                    neg_ids.append(neg)
                neg_ids += [0] * pad_len

                yield (
                    {"item_ids": input_ids, "category_ids": cat_ids, "padding_mask": mask},
                    {"positive_sequence": pos_ids, "negative_sequence": neg_ids}
                )

    return tf.data.Dataset.from_generator(
        generator,
        output_signature=(
            {
                "item_ids": tf.TensorSpec((max_len,), tf.int32),
                "category_ids": tf.TensorSpec((max_len,), tf.int32),
                "padding_mask": tf.TensorSpec((max_len,), tf.bool),
            },
            {
                "positive_sequence": tf.TensorSpec((max_len,), tf.int32),
                "negative_sequence": tf.TensorSpec((max_len,), tf.int32),
            }
        )
    ).repeat().batch(BATCH_SIZE).prefetch(tf.data.AUTOTUNE)

# ======================================================
# MAIN
# ======================================================
def main():
    print("🚀 TRAINING CHẾ ĐỘ: HIGH PERFORMANCE (Báo cáo đồ án)")
    
    item_seqs, cat_seqs, last_times, vocab_size, num_categories = load_data()
    popular_items = get_popular_items(item_seqs)

    # Train/Test Split
    train_items, train_cats = [], []
    test_set = []

    print("✂️ Creating Train/Test Split...")
    for seq, cat, ts in zip(item_seqs, cat_seqs, last_times):
        train_items.append(seq[:-1])
        train_cats.append(cat[:-1])
        test_set.append({
            "input_items": seq[:-1],
            "input_cats": cat[:-1],
            "label": seq[-1],
            "test_time": ts
        })

    with open(TEST_SET_SAVE_PATH, 'wb') as f:
        pickle.dump(test_set, f)

    # Dataset
    train_ds = create_dataset(train_items, train_cats, MAX_LEN, vocab_size, popular_items)

    # MODEL
    print("🏗️ Building SASRec Model (2 Layers, 4 Heads)...")
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

    optimizer = tf.keras.optimizers.AdamW(learning_rate=lr_schedule, weight_decay=0.01)
    model.compile(optimizer=optimizer)

    callbacks = [
        tf.keras.callbacks.ModelCheckpoint(
            MODEL_SAVE_PATH,
            save_best_only=True,
            monitor='loss',
            mode='min',
            save_freq='epoch',
            verbose=1
        ),
        tf.keras.callbacks.EarlyStopping(patience=8, restore_best_weights=True, monitor='loss')
    ]

    print(f"🚀 Training start: {MAX_USERS} users, {EPOCHS} epochs...")
    history = model.fit(
        train_ds, 
        epochs=EPOCHS, 
        steps_per_epoch=STEPS_PER_EPOCH,
        callbacks=callbacks
    )

    try:
        with open(LOG_PATH, 'w') as f:
            json.dump(history.history, f)
        print("📊 Saved training log.")
    except Exception as e:
        print(f"⚠️ Cannot save log: {e}")

if __name__ == "__main__":
    main()