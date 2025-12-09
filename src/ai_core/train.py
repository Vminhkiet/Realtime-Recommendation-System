import pickle
import numpy as np
import tensorflow as tf
import random
import os
from model import SasRec  # Import class SasRec từ file trên

# --- CẤU HÌNH ---
# Sử dụng đường dẫn tuyệt đối để tránh lỗi FileNotFoundError
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(BASE_DIR))

DATA_PATH = os.path.join(PROJECT_ROOT, 'data/model_registry/dataset.pkl')
MAP_PATH = os.path.join(PROJECT_ROOT, 'data/model_registry/item_map.pkl')
MODEL_SAVE_PATH = os.path.join(PROJECT_ROOT, 'data/model_registry/sasrec_v1.keras')

# Hyperparameters
MAX_LEN = 50
BATCH_SIZE = 32  # Giảm xuống 64 cho nhẹ máy
EMBED_DIM = 32
EPOCHS = 5      # Có thể tăng lên 20-50 nếu Loss vẫn cao

def load_data():
    print("📥 Đang load dữ liệu...")
    if not os.path.exists(DATA_PATH):
        raise FileNotFoundError(f"❌ Không tìm thấy file {DATA_PATH}. Hãy chạy data_process.py trước!")
        
    with open(DATA_PATH, 'rb') as f:
        sequences = pickle.load(f)
    with open(MAP_PATH, 'rb') as f:
        item2id, _ = pickle.load(f)
    return sequences, len(item2id)

def prepare_training_data(sequences, max_len, num_items):
    def generator():
        for seq in sequences:
            # 1. Cắt chuỗi
            seq = seq[-(max_len + 1):]
            if len(seq) < 2: continue

            # 2. Tách Input/Target
            input_seq = seq[:-1]
            pos_seq = seq[1:]
            
            # 3. Padding
            pad_len = max_len - len(input_seq)
            input_ids = [0] * pad_len + input_seq
            pos_ids = [0] * pad_len + pos_seq
            mask = [False] * pad_len + [True] * len(input_seq)
            
            # 4. Negative Sampling
            neg_ids = []
            seq_set = set(seq)
            for _ in range(len(input_seq)):
                neg = random.randint(1, num_items)
                while neg in seq_set: 
                    neg = random.randint(1, num_items)
                neg_ids.append(neg)
            neg_ids = [0] * pad_len + neg_ids

            yield (
                {"item_ids": input_ids, "padding_mask": mask},
                {"positive_sequence": pos_ids, "negative_sequence": neg_ids}
            )

    return tf.data.Dataset.from_generator(
        generator,
        output_signature=(
            {
                "item_ids": tf.TensorSpec(shape=(max_len,), dtype=tf.int32),
                "padding_mask": tf.TensorSpec(shape=(max_len,), dtype=tf.bool)
            },
            {
                "positive_sequence": tf.TensorSpec(shape=(max_len,), dtype=tf.int32),
                "negative_sequence": tf.TensorSpec(shape=(max_len,), dtype=tf.int32)
            }
        )
    ).shuffle(1000).batch(BATCH_SIZE).cache().prefetch(tf.data.AUTOTUNE)

def main():
    # 1. Chuẩn bị dữ liệu
    sequences, num_items = load_data()
    print(f"✅ Tổng số user sessions: {len(sequences)}")
    print(f"✅ Tổng số sản phẩm: {num_items}")
    
    train_ds = prepare_training_data(sequences, MAX_LEN, num_items)

    # 2. Khởi tạo Model
    # +1 vào vocab size cho padding (0)
    model = SasRec(
        vocabulary_size=num_items + 1, 
        num_layers=2,
        num_heads=2,
        hidden_dim=EMBED_DIM,
        dropout=0.1,
        max_sequence_length=MAX_LEN
    )

    # 3. Compile
    # AdamW thường hội tụ tốt hơn Adam thường cho Transformer
    model.compile(optimizer=tf.keras.optimizers.AdamW(learning_rate=0.001))

    # 4. Train
    print("🚀 Bắt đầu Training...")
    history = model.fit(train_ds, epochs=EPOCHS)

    # 5. Lưu Model (Giờ sẽ không còn lỗi NotImplementedError nữa)
    os.makedirs(os.path.dirname(MODEL_SAVE_PATH), exist_ok=True)
    model.save(MODEL_SAVE_PATH)
    print(f"💾 Đã lưu model thành công tại: {MODEL_SAVE_PATH}")

if __name__ == "__main__":
    main()