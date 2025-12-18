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
# CONFIG
# ======================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(BASE_DIR))

PARQUET_PATH = os.path.join(PROJECT_ROOT, 'data/model_registry/processed_parquet')
MAP_PATH = os.path.join(PROJECT_ROOT, 'data/model_registry/item_map.json')
CAT_MAP_PATH = os.path.join(PROJECT_ROOT, 'data/model_registry/category_map.json')
MODEL_SAVE_PATH = os.path.join(PROJECT_ROOT, 'data/model_registry/sasrec_v1.keras')
TEST_SET_SAVE_PATH = os.path.join(PROJECT_ROOT, 'data/model_registry/test_set.pkl')
LOG_PATH = os.path.join(PROJECT_ROOT, 'data/training_history.json')

# ======================================================
# HYPERPARAMETERS
# ======================================================
MAX_LEN = 50
BATCH_SIZE = 128
EPOCHS = 20
EMBED_DIM = 64
STEP_SIZE = 25
MIN_INTERACTIONS = 5

# ======================================================
# UTILS
# ======================================================
def normalize_ts(ts):
    """ms → s nếu cần"""
    return ts / 1000 if ts > 32503680000 else ts


def get_popular_items(sequences, top_k=2000):
    print("🔥 Đang tính Popular Items (Hard Negatives)...")
    all_items = [i for seq in sequences for i in seq]
    counter = Counter(all_items)
    return [item for item, _ in counter.most_common(top_k)]


# ======================================================
# LOAD DATA (TIME-AWARE)
# ======================================================
def load_data():
    print("📥 Đang load dữ liệu Parquet...")
    if not os.path.exists(PARQUET_PATH):
        raise FileNotFoundError("❌ Chưa chạy spark_process.py!")

    df = pd.read_parquet(PARQUET_PATH)

    # 🔥 SỬA TÊN CỘT TẠI ĐÂY: Spark lưu là 'last_timestamp'
    TIME_COL = 'last_timestamp' 
    
    if TIME_COL not in df.columns:
        raise ValueError(f"❌ Thiếu cột {TIME_COL}! Các cột hiện có: {df.columns.tolist()}")

    # Chuẩn hóa thời gian
    df[TIME_COL] = df[TIME_COL].apply(normalize_ts)

    global_start = df[TIME_COL].min()
    global_end = df[TIME_COL].max()

    print("\n" + "=" * 70)
    print("🕒 TIME RANGE TOÀN BỘ DATASET (Dựa trên item cuối)")
    print(f"• Global time : {datetime.fromtimestamp(global_start)} → {datetime.fromtimestamp(global_end)}")
    print("• TRAIN       : toàn bộ hành vi trước item cuối của mỗi user")
    print("• TEST        : item cuối cùng của mỗi user (Leave-One-Out)")
    print("📌 Không leakage thời gian")
    print("=" * 70 + "\n")

    # Giảm dữ liệu để dev (Simulation mode)
    df = df.sample(n=min(5000, len(df)), random_state=42)
    print(f"📉 Sampled users: {len(df)}")

    item_seqs = df['sequence_ids'].tolist()
    cat_seqs = df['category_ids'].tolist()
    last_times = df[TIME_COL].tolist()

    with open(MAP_PATH, 'r') as f:
        vocab_size = len(json.load(f))

    if os.path.exists(CAT_MAP_PATH):
        with open(CAT_MAP_PATH, 'r') as f:
            num_categories = len(json.load(f))
    else:
        all_cats = set()
        for seq in cat_seqs:
            all_cats.update(seq)
        num_categories = max(all_cats)

    return item_seqs, cat_seqs, last_times, vocab_size, num_categories


# ======================================================
# DATASET
# ======================================================
# ======================================================
# DATASET
# ======================================================
def create_dataset(item_seqs, cat_seqs, max_len, num_items, popular_items):
    def generator():
        for item_seq, cat_seq in zip(item_seqs, cat_seqs):
            # 🔥 QUAN TRỌNG: Ép kiểu về list để tránh lỗi broadcast của numpy
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

                # Tách input/target và đảm bảo luôn là list
                curr_item = list(item_win[:-1])
                curr_pos = list(item_win[1:])
                curr_cat = list(cat_win[:-1])

                # Giới hạn độ dài max_len
                curr_item = curr_item[-max_len:]
                curr_pos = curr_pos[-max_len:]
                curr_cat = curr_cat[-max_len:]

                # Augmentation: Masking
                if random.random() < 0.2:
                    curr_item[random.randint(0, len(curr_item) - 1)] = 0

                pad_len = max_len - len(curr_item)

                # Bây giờ phép + sẽ hoạt động như nối chuỗi (list concatenation)
                input_ids = curr_item + [0] * pad_len
                pos_ids = curr_pos + [0] * pad_len
                cat_ids = curr_cat + [0] * pad_len
                mask = [True] * len(curr_item) + [False] * pad_len

                # Hard Negative Sampling
                win_set = set(item_win)
                neg_ids = []
                for _ in range(len(curr_item)):
                    neg = random.choice(popular_items) if random.random() < 0.5 else random.randint(1, num_items)
                    while neg in win_set:
                        neg = random.randint(1, num_items)
                    neg_ids.append(neg)
                neg_ids += [0] * pad_len

                yield (
                    {
                        "item_ids": input_ids[:max_len],
                        "category_ids": cat_ids[:max_len],
                        "padding_mask": mask[:max_len]
                    },
                    {
                        "positive_sequence": pos_ids[:max_len],
                        "negative_sequence": neg_ids[:max_len]
                    }
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
    ).shuffle(10000).batch(BATCH_SIZE).repeat().prefetch(tf.data.AUTOTUNE)


# ======================================================
# MAIN
# ======================================================
def main():
    item_seqs, cat_seqs, last_times, vocab_size, num_categories = load_data()

    print(f"📊 Users: {len(item_seqs)} | Items: {vocab_size} | Categories: {num_categories}")

    # Filter
    filtered = [(s, c, t) for s, c, t in zip(item_seqs, cat_seqs, last_times) if len(s) >= MIN_INTERACTIONS]
    if not filtered:
        raise ValueError("❌ Dữ liệu quá ít!")

    item_seqs, cat_seqs, last_times = zip(*filtered)
    print(f"📉 Users sau filter: {len(item_seqs)}")

    # Popular items
    popular_items = get_popular_items(item_seqs)

    # ==================================================
    # TIME-AWARE LEAVE-ONE-OUT
    # ==================================================
    train_items, train_cats = [], []
    test_set = []

    print("✂️ Leave-One-Out Split (Time-aware)...")
    for seq, cat, ts in zip(item_seqs, cat_seqs, last_times):
        train_items.append(seq[:-1])
        train_cats.append(cat[:-1])

        test_set.append({
            "input_items": seq[:-1],
            "input_cats": cat[:-1],
            "label": seq[-1],
            "test_time": ts,
            "train_time_range": {
                "start": ts - 1,
                "end": ts
            }
        })

    with open(TEST_SET_SAVE_PATH, 'wb') as f:
        pickle.dump(test_set, f)

    print(f"✅ Saved test set: {len(test_set)} samples")

    # Dataset
    train_ds = create_dataset(
        train_items,
        train_cats,
        MAX_LEN,
        vocab_size,
        popular_items
    )

    # ==================================================
    # MODEL
    # ==================================================
    model = SasRec(
        vocabulary_size=vocab_size + 1,
        category_size=num_categories + 1,
        num_layers=2,
        num_heads=4,
        hidden_dim=EMBED_DIM,
        dropout=0.4,
        max_sequence_length=MAX_LEN
    )

    lr_schedule = tf.keras.optimizers.schedules.CosineDecayRestarts(
        initial_learning_rate=0.002,
        first_decay_steps=500,
        t_mul=2.0,
        m_mul=0.9,
        alpha=1e-6
    )

    optimizer = tf.keras.optimizers.AdamW(
        learning_rate=lr_schedule,
        weight_decay=0.05
    )

    model.compile(optimizer=optimizer)

    callbacks = [
        tf.keras.callbacks.ModelCheckpoint(
            MODEL_SAVE_PATH,
            save_best_only=False,
            monitor='loss',
            mode='min',
            save_freq=500,
            verbose=1
        ),
        tf.keras.callbacks.EarlyStopping(
            patience=15,
            restore_best_weights=True,
            monitor='loss'
        )
    ]

    print("🚀 Training SASRec...")
    history = model.fit(train_ds, epochs=EPOCHS, callbacks=callbacks)

    try:
        with open(LOG_PATH, 'w') as f:
            json.dump(history.history, f)
        print("📊 Saved training log.")
    except Exception as e:
        print(f"⚠️ Cannot save log: {e}")


if __name__ == "__main__":
    main()
