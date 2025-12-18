import os
import json
import pickle
import numpy as np
import tensorflow as tf
from tqdm import tqdm

try:
    from .model import SasRec
except ImportError:
    from model import SasRec

# --- CẤU HÌNH ---
MAX_LEN = 50 
NUM_NEG_TEST = 99 # 1 món đúng + 99 món sai = 100 món để xếp hạng

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(BASE_DIR))
MODEL_PATH = os.path.join(PROJECT_ROOT, 'data/model_registry/sasrec_v1.keras')
TEST_SET_PATH = os.path.join(PROJECT_ROOT, 'data/model_registry/test_set.pkl')
MAP_PATH = os.path.join(PROJECT_ROOT, 'data/model_registry/item_map.json')

def main():
    print("📊 ĐÁNH GIÁ VỚI CHIẾN THUẬT SAMPLED METRICS (1 vs 99)...")
    
    # 1. Load Resources
    model = tf.keras.models.load_model(MODEL_PATH)
    with open(TEST_SET_PATH, 'rb') as f:
        test_set = pickle.load(f)
    with open(MAP_PATH, 'r') as f:
        item_map = json.load(f)
        vocab_size = len(item_map)

    hits_10 = 0
    ndcgs_10 = 0
    
    # 2. Đánh giá từng User (Sampled evaluation không dùng batch predict được dễ dàng)
    print(f"🚀 Đang chấm điểm cho {len(test_set)} users...")
    
    for sample in tqdm(test_set):
        seq = sample['input_items'][-MAX_LEN:]
        cat = sample['input_cats'][-MAX_LEN:]
        target = sample['label']
        
        # Tạo danh sách 100 món cần xếp hạng
        test_items = [target]
        while len(test_items) < 100:
            neg = np.random.randint(1, vocab_size + 1)
            if neg != target:
                test_items.append(neg)
        
        # Chuẩn bị Input cho Model
        pad_len = MAX_LEN - len(seq)
        in_item = np.array([list(seq) + [0] * pad_len])
        in_cat = np.array([list(cat) + [0] * pad_len])
        in_mask = np.array([[True] * len(seq) + [False] * pad_len])
        
        # Lấy Embedding của chuỗi hành vi từ Model
        outputs = model.predict({
            "item_ids": in_item,
            "category_ids": in_cat,
            "padding_mask": in_mask
        }, verbose=0)
        
        # Thay vì lấy Top 10 toàn cục, ta chỉ lấy điểm của 100 món này
        # Lấy vector đặc trưng cuối cùng của sequence
        seq_emb = outputs['item_sequence_embedding'][0] # [Seq, Dim]
        # Lấy embedding tại vị trí cuối cùng có data
        last_idx = len(seq) - 1
        last_emb = seq_emb[last_idx] # [Dim]
        
        # Lấy trọng số Embedding của 100 món test
        all_item_weights = model.item_embedding.embeddings # [Vocab, Dim]
        test_items_idx = np.array(test_items)
        # Lấy embedding của 100 món
        test_embs = tf.gather(all_item_weights, test_items_idx) # [100, Dim]
        
        # Tính điểm Score = Dot Product
        scores = tf.matmul(test_embs, tf.expand_dims(last_emb, -1))
        scores = tf.squeeze(scores).numpy() # [100]
        
        # Xếp hạng: Món đúng (index 0) đứng thứ mấy trong 100 món?
        # Điểm càng cao hạng càng nhỏ (hạng 0 là cao nhất)
        rank = (scores > scores[0]).sum()
        
        if rank < 10:
            hits_10 += 1
            ndcgs_10 += 1 / np.log2(rank + 2)

    avg_hr = hits_10 / len(test_set)
    avg_ndcg = ndcgs_10 / len(test_set)
    
    print("\n-------------------------------------------------")
    print(f"🏆 KẾT QUẢ SAMPLED (Hit@10 trong 100 món):")
    print(f"   ✅ Hit Rate @ 10: {avg_hr:.4f} ({avg_hr*100:.2f}%)")
    print(f"   ⭐ NDCG @ 10    : {avg_ndcg:.4f}")
    print("-------------------------------------------------")

if __name__ == "__main__":
    main()