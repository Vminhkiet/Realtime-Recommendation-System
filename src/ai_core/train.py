import os
import json
import random
import pickle
import pandas as pd
import tensorflow as tf
from collections import Counter
from model import SasRec

# --- CẤU HÌNH ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(BASE_DIR))

PARQUET_PATH = os.path.join(PROJECT_ROOT, 'data/model_registry/processed_parquet')
MAP_PATH = os.path.join(PROJECT_ROOT, 'data/model_registry/item_map.json')
CAT_MAP_PATH = os.path.join(PROJECT_ROOT, 'data/model_registry/category_map.json')
MODEL_SAVE_PATH = os.path.join(PROJECT_ROOT, 'data/model_registry/sasrec_v1.keras')
TEST_SET_SAVE_PATH = os.path.join(PROJECT_ROOT, 'data/model_registry/test_set.pkl')
LOG_PATH = os.path.join(PROJECT_ROOT, 'data/training_history.json')

# --- HYPERPARAMETERS TỐI ƯU (GRANDMASTER MODE) ---
MAX_LEN = 50
BATCH_SIZE = 32
EPOCHS = 100
EMBED_DIM = 128
STEP_SIZE = 1
MIN_INTERACTIONS = 5

# 🔥 Hàm lấy hàng Hot (Hard Negative)
def get_popular_items(sequences, top_k=2000):
    print("🔥 Đang tính toán độ phổ biến của sản phẩm (Hard Negatives)...")
    all_items = [item for seq in sequences for item in seq]
    counter = Counter(all_items)
    popular_items = [item for item, count in counter.most_common(top_k)]
    return popular_items

def load_data():
    print("📥 Đang load dữ liệu từ Parquet...")
    if not os.path.exists(PARQUET_PATH):
        raise FileNotFoundError("❌ Chưa chạy spark_process.py!")
    
    df = pd.read_parquet(PARQUET_PATH)
    item_seqs = df['sequence_ids'].tolist()

    # 🔥 Giảm dung lượng data cho việc phát triển nhanh (Chỉ dùng 5000 users)
    df = df.sample(n=min(5000, len(df)), random_state=42) 
    print(f"📉 Đã lấy mẫu simulation: {len(df)} users")
    
    if 'category_ids' not in df.columns:
        raise ValueError("❌ Thiếu cột category_ids. Hãy chạy lại spark_process.py!")
        
    cat_seqs = df['category_ids'].tolist()
    
    with open(MAP_PATH, 'r') as f:
        item_map = json.load(f)
        vocab_size = len(item_map)

    if os.path.exists(CAT_MAP_PATH):
        with open(CAT_MAP_PATH, 'r') as f:
            cat_map = json.load(f)
            num_categories = len(cat_map)
    else:
        print("⚠️ Không tìm thấy category_map.json. Tự tính toán...")
        all_cats = set()
        for seq in cat_seqs: all_cats.update(seq)
        num_categories = max(all_cats)

    return item_seqs, cat_seqs, vocab_size, num_categories

def create_dataset(item_seqs, cat_seqs, max_len, num_items, num_cats, popular_items):
    def generator():
        for item_seq, cat_seq in zip(item_seqs, cat_seqs):
            seq_len = len(item_seq)
            
            if seq_len <= max_len + 1:
                item_windows = [item_seq]
                cat_windows = [cat_seq]
            else:
                item_windows = [item_seq[i : i + max_len + 1] for i in range(0, seq_len - max_len, STEP_SIZE)]
                cat_windows = [cat_seq[i : i + max_len + 1] for i in range(0, seq_len - max_len, STEP_SIZE)]

            for item_window, cat_window in zip(item_windows, cat_windows):
                if len(item_window) < 2: continue
                
                input_item = list(item_window[:-1])
                pos_item = list(item_window[1:])
                input_cat = list(cat_window[:-1])
                
                # 🔥 KỸ THUẬT DATA AUGMENTATION: MASKING (Che mắt) 🔥
                # Tỷ lệ 20% số mẫu sẽ bị che mất 1 món đồ ngẫu nhiên
                # Giúp Model không bị học tủ, phải suy luận từ ngữ cảnh
                if random.random() < 0.2: 
                    mask_idx = random.randint(0, len(input_item) - 1)
                    input_item[mask_idx] = 0 # Gán về 0 (Xem như chưa từng mua)
                    # Không mask category để model vẫn còn manh mối suy luận
                
                pad_len = max_len - len(input_item)
                
                input_ids = input_item + [0] * pad_len
                cat_ids = input_cat + [0] * pad_len
                pos_ids = pos_item + [0] * pad_len
                mask = [True] * len(input_item) + [False] * pad_len
                
                # --- Hard Negative Sampling ---
                neg_ids = []
                win_set = set(item_window)
                for _ in range(len(input_item)):
                    if random.random() < 0.5:
                        neg = random.choice(popular_items)
                    else:
                        neg = random.randint(1, num_items)
                    while neg in win_set: 
                        neg = random.randint(1, num_items)
                    neg_ids.append(neg)
                neg_ids = neg_ids + [0] * pad_len
                
                yield (
                    {
                        "item_ids": input_ids, 
                        "category_ids": cat_ids, 
                        "padding_mask": mask
                    },
                    {
                        "positive_sequence": pos_ids, 
                        "negative_sequence": neg_ids
                    }
                )

    return tf.data.Dataset.from_generator(
        generator,
        output_signature=(
            {
                "item_ids": tf.TensorSpec(shape=(max_len,), dtype=tf.int32),
                "category_ids": tf.TensorSpec(shape=(max_len,), dtype=tf.int32),
                "padding_mask": tf.TensorSpec(shape=(max_len,), dtype=tf.bool)
            },
            {
                "positive_sequence": tf.TensorSpec(shape=(max_len,), dtype=tf.int32), 
                "negative_sequence": tf.TensorSpec(shape=(max_len,), dtype=tf.int32)
            }
        )
    ).cache().shuffle(10000).batch(BATCH_SIZE).prefetch(tf.data.AUTOTUNE)

def main():
    # 1. Load Data
    item_seqs, cat_seqs, vocab_size, num_categories = load_data()
    print(f"📊 Tổng User: {len(item_seqs)}")
    print(f"📊 Tổng Item: {vocab_size} | Tổng Category: {num_categories}")

    # 2. Lọc Dữ Liệu
    print(f"🧹 Đang lọc user mua dưới {MIN_INTERACTIONS} món...")
    filtered_data = [(s, c) for s, c in zip(item_seqs, cat_seqs) if len(s) >= MIN_INTERACTIONS]
    
    if not filtered_data: raise ValueError("❌ Lỗi: Dữ liệu quá ít!")

    item_seqs = [x[0] for x in filtered_data]
    cat_seqs = [x[1] for x in filtered_data]
    print(f"📉 User còn lại: {len(item_seqs)}")

    # 3. Tính Popular Items
    popular_items = get_popular_items(item_seqs, top_k=2000)

    # 4. Split Train/Test
    print("✂️ Leave-One-Out Split...")
    train_items, train_cats = [], []
    test_set = []

    for i in range(len(item_seqs)):
        seq = item_seqs[i]
        cat = cat_seqs[i]
        
        target_item = seq[-1]
        train_items.append(seq[:-1])
        train_cats.append(cat[:-1]) 
        
        test_set.append({
            "input_items": seq[:-1],
            "input_cats": cat[:-1],
            "label": target_item
        })

    with open(TEST_SET_SAVE_PATH, 'wb') as f: pickle.dump(test_set, f)
    print(f"✅ Đã lưu {len(test_set)} mẫu Test.")

    # 5. Tạo Dataset
    train_ds = create_dataset(train_items, train_cats, MAX_LEN, vocab_size, num_categories, popular_items)

    # 6. Train Model (Cấu hình Grandmaster)
    print("🚀 Bắt đầu Training (Augmentation + Cosine Decay)...")
    
    model = SasRec(
        vocabulary_size=vocab_size + 1, 
        category_size=num_categories + 1,
        num_layers=2, 
        num_heads=4,
        hidden_dim=EMBED_DIM, 
        dropout=0.4, # ⬆️ Tăng Dropout lên 0.4 để ép model học khó hơn
        max_sequence_length=MAX_LEN
    )
    
    # 🔥 OPTIMIZER: Dùng CosineDecayRestarts thay vì giảm đều
    # Giúp model thoát khỏi các điểm cực tiểu cục bộ (Local Minima)
    lr_schedule = tf.keras.optimizers.schedules.CosineDecayRestarts(
        initial_learning_rate=0.001,
        first_decay_steps=1000,
        t_mul=2.0,
        m_mul=0.9,
        alpha=1e-6
    )
    
    optimizer = tf.keras.optimizers.AdamW(
        learning_rate=lr_schedule, 
        weight_decay=0.05 # ⬆️ Tăng Weight Decay lên 0.05 để phạt nặng việc học tủ
    )

    callbacks = [
        # Bỏ ReduceLROnPlateau vì đã có Scheduler xịn ở trên
        tf.keras.callbacks.ModelCheckpoint(filepath=MODEL_SAVE_PATH, save_best_only=True, monitor='loss', mode='min', verbose=1),
        tf.keras.callbacks.EarlyStopping(patience=15, restore_best_weights=True) # Tăng kiên nhẫn lên 15
    ]

    model.compile(optimizer=optimizer)
    history = model.fit(train_ds, epochs=EPOCHS, callbacks=callbacks)

    print(f"📊 Đang lưu log training vào: {LOG_PATH}")
    try:
        with open(LOG_PATH, 'w') as f: json.dump(history.history, f)
    except Exception as e: print(f"⚠️ Lỗi lưu log: {e}")

if __name__ == "__main__":
    main()