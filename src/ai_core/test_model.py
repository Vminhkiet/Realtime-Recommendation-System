import os
import json
import pickle
import tensorflow as tf
import numpy as np
import random

# --- CẤU HÌNH ---
# Import class SasRec từ file model.py cùng thư mục
try:
    from .model import SasRec
except ImportError:
    from model import SasRec

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(BASE_DIR))

# Load các file tài nguyên đã train
MODEL_PATH = os.path.join(PROJECT_ROOT, 'data/model_registry/sasrec_v1.keras')
MAP_PATH = os.path.join(PROJECT_ROOT, 'data/model_registry/item_map.json')
TEST_SET_PATH = os.path.join(PROJECT_ROOT, 'data/model_registry/test_set.pkl')

MAX_LEN = 50

def main():
    print("🧪 BẮT ĐẦU KIỂM TRA MODEL (OFFLINE SANITY CHECK)...")
    
    # 1. Kiểm tra file tồn tại
    if not os.path.exists(MODEL_PATH):
        print(f"❌ Lỗi: Không tìm thấy model tại {MODEL_PATH}")
        print("   -> Bạn đã chạy 'make train' chưa?")
        return

    # 2. Load Resources
    print("📥 Đang load Model & Map...")
    try:
        model = tf.keras.models.load_model(MODEL_PATH)
        
        with open(MAP_PATH, 'r') as f:
            id2item = {int(k): v for k, v in json.load(f).items()}
            
        with open(TEST_SET_PATH, 'rb') as f:
            test_set = pickle.load(f)
            
        print(f"✅ Load xong. Vocab size: {len(id2item)}")
    except Exception as e:
        print(f"❌ Lỗi load tài nguyên: {e}")
        return

    # 3. Lấy ngẫu nhiên 1 mẫu trong tập Test để thử
    if not test_set:
        print("❌ Tập test rỗng!")
        return
        
    sample = random.choice(test_set)
    
    # 🔥 CẬP NHẬT: Lấy cả Item và Category từ mẫu test mới
    # (Cấu trúc mới trong train_model.py là 'input_items' và 'input_cats')
    try:
        history_items = sample['input_items']
        history_cats = sample['input_cats']
        truth = sample['label']
    except KeyError:
        print("❌ Lỗi format data: File test_set.pkl có vẻ là phiên bản cũ.")
        print("👉 Hãy chạy lại 'make train-ai' để sinh file test mới nhất.")
        return

    print("\n-------------------------------------------------")
    print(f"👤 USER HISTORY (5 món gần nhất):")
    # Chỉ hiển thị tên Item (Category để model dùng ngầm bên dưới)
    for item_id in history_items[-5:]:
        print(f"   - {id2item.get(item_id, 'Unknown')}")
        
    truth_name = id2item.get(truth, 'Unknown')
    print(f"\n🎯 GROUND TRUTH (Thực tế mua): {truth_name} (ID: {truth})")
    
    # 4. Dự đoán (Inference)
    # Preprocessing
    pad_len = MAX_LEN - len(history_items)
    
    # Padding cho cả Item và Category
    input_ids = list(history_items) + [0] * pad_len
    cat_ids = list(history_cats) + [0] * pad_len # 🔥 Padding Category
    mask = [True] * len(history_items) + [False] * pad_len
    
    # Tạo input dictionary đúng chuẩn Model mới
    inp = {
        "item_ids": tf.constant([input_ids]),
        "category_ids": tf.constant([cat_ids]), # 🔥 Thêm input này
        "padding_mask": tf.constant([mask])
    }
    
    # Predict
    print("\n🤖 MODEL PREDICTION (Top 10):")
    try:
        output = model.predict(inp, verbose=0)
        top_k_indices = output['predictions'][0]
        
        found = False
        for rank, idx in enumerate(top_k_indices):
            idx = int(idx)
            name = id2item.get(idx, f"Unknown_ID_{idx}")
            
            is_correct = (idx == truth)
            mark = "✅ CHÍNH XÁC!" if is_correct else ""
            if is_correct: found = True
            
            print(f"   #{rank+1}: {name} {mark}")

        print("-------------------------------------------------")
        if found:
            print("🎉 KẾT QUẢ: Model dự đoán ĐÚNG!")
        else:
            print("⚠️ KẾT QUẢ: Model dự đoán SAI (Cần train thêm hoặc chỉnh tham số).")
            
    except Exception as e:
        print(f"❌ Lỗi khi Predict: {e}")
        print("💡 Gợi ý: Kiểm tra xem Model đã được build với Category Embedding chưa?")

if __name__ == "__main__":
    main()