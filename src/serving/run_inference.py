import sys
import os
import json
import numpy as np
import tensorflow as tf
from kafka import KafkaConsumer
from collections import defaultdict

# ==========================================
# 1. CẤU HÌNH & ĐƯỜNG DẪN
# ==========================================
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
if project_root not in sys.path:
    sys.path.append(project_root)

# Paths
MODEL_REGISTRY = os.path.join(project_root, 'data/model_registry')
ITEM_MAP_PATH = os.path.join(MODEL_REGISTRY, 'item_map.json')
CAT_MAP_PATH = os.path.join(MODEL_REGISTRY, 'category_map.json')
MODEL_PATH = os.path.join(MODEL_REGISTRY, 'sasrec_v1.keras')

# Config Kafka
KAFKA_TOPIC = 'user_clicks'
BOOTSTRAP_SERVERS = os.getenv('KAFKA_SERVER', 'kafka:29092')

# Model Params
MAX_LEN = 50

# ==========================================
# 2. CLASS XỬ LÝ CHÍNH (IN-MEMORY VERSION)
# ==========================================
class RecommendationEngine:
    def __init__(self):
        print("🛠️  Đang khởi tạo AI Engine (RAM Mode - No Redis)...")
        
        # 🟢 THAY THẾ REDIS BẰNG DICTIONARY
        # Cấu trúc: {'user_1': [1, 2, 3], 'user_2': [5, 6]}
        self.user_history = defaultdict(list)

        self.model = None
        self.item_map = {}      
        self.rev_item_map = {}  
        self.cat_map = {}       
        self.item_embeddings = None 

        self.load_resources()

    def load_resources(self):
        try:
            print("🔄 Loading Model Resources...")
            
            # --- 1. Load Item Map ---
            if not os.path.exists(ITEM_MAP_PATH):
                raise FileNotFoundError(f"Thiếu file: {ITEM_MAP_PATH}")

            with open(ITEM_MAP_PATH, 'r') as f:
                raw_map = json.load(f)
            
            # Auto-detect format map
            first_k, _ = list(raw_map.items())[0]
            if str(first_k).isdigit(): 
                self.rev_item_map = {int(k): v for k, v in raw_map.items()}
                self.item_map = {v: int(k) for k, v in raw_map.items()}
            else:
                self.item_map = {k: int(v) for k, v in raw_map.items()}
                self.rev_item_map = {int(v): k for k, v in raw_map.items()}
            
            print(f"✅ Loaded {len(self.item_map)} items.")

            # --- 2. Load Category Map (ĐÃ SỬA LỖI CRASH) ---
            if os.path.exists(CAT_MAP_PATH):
                with open(CAT_MAP_PATH, 'r') as f:
                    raw_cat = json.load(f)
                    self.cat_map = {}
                    for k, v in raw_cat.items():
                        try:
                            # Chỉ lấy nếu value là số
                            self.cat_map[int(k)] = int(v)
                        except ValueError:
                            # Nếu gặp chữ (vd 'Other_Gaming'), gán = 0
                            self.cat_map[int(k)] = 0
            else:
                print("⚠️ Warning: Không tìm thấy category map.")

            # --- 3. Load Model ---
            try:
                from src.ai_core.model import SasRec
            except ImportError:
                from src.model import SasRec

            if not os.path.exists(MODEL_PATH):
                 raise FileNotFoundError(f"Thiếu file model: {MODEL_PATH}")

            self.model = tf.keras.models.load_model(MODEL_PATH, custom_objects={'SasRec': SasRec})
            self.item_embeddings = self.model.item_embedding.embeddings 
            
            print("✅ AI Engine đã sẵn sàng!")
            
        except Exception as e:
            print(f"❌ ERROR loading resources: {e}")
            sys.exit(1)

    def predict(self, user_id, curr_item_val):
        if not self.model: return []

        # --- BƯỚC 1: CHUẨN HÓA ITEM ID ---
        curr_item_idx = None
        curr_item_str = str(curr_item_val)

        if curr_item_str.isdigit():
            idx = int(curr_item_str)
            if idx in self.rev_item_map:
                curr_item_idx = idx
            else:
                return []
        else:
            curr_item_idx = self.item_map.get(curr_item_str)
            if not curr_item_idx:
                return []

        # --- BƯỚC 2: CẬP NHẬT RAM (Thay vì Redis) ---
        # Lấy lịch sử hiện tại của user
        history = self.user_history[user_id]
        
        # Thêm item mới vào cuối
        history.append(curr_item_idx)
        
        # Cắt nếu dài quá 50
        if len(history) > MAX_LEN:
            history = history[-MAX_LEN:]
        
        # Lưu lại vào RAM
        self.user_history[user_id] = history
        
        # Dùng history này để predict
        history_seq = history

        # --- BƯỚC 3: INPUT MODEL ---
        cat_seq = [self.cat_map.get(i, 0) for i in history_seq]
        pad_len = MAX_LEN - len(history_seq)
        
        input_ids = np.array([[0] * pad_len + history_seq], dtype=np.int32)
        input_cats = np.array([[0] * pad_len + cat_seq], dtype=np.int32)
        
        mask_arr = [False] * pad_len + [True] * len(history_seq)
        input_mask = np.array([mask_arr], dtype=bool)

        inputs = {
            "item_ids": tf.constant(input_ids),
            "category_ids": tf.constant(input_cats),
            "padding_mask": tf.constant(input_mask)
        }

        # --- BƯỚC 4: INFERENCE ---
        outputs = self.model(inputs, training=False)
        user_embedding = outputs['item_sequence_embedding'][:, -1, :] 

        scores = tf.matmul(user_embedding, self.item_embeddings, transpose_b=True)
        scores = scores.numpy().flatten()

        scores[curr_item_idx] = -np.inf # Không gợi ý lại cái vừa xem

        top_indices = np.argpartition(scores, -5)[-5:] 
        top_indices = top_indices[np.argsort(scores[top_indices])][::-1]

        recommendations = []
        for idx in top_indices:
            if idx == 0: continue
            item_name = self.rev_item_map.get(idx, f"Unknown_{idx}")
            recommendations.append(item_name)

        return recommendations

# ==========================================
# 3. MAIN LOOP
# ==========================================
def main():
    print("\n" + "="*60)
    print("🎧 KAFKA CONSUMER (NO REDIS MODE)")
    print(f"👉 Listening on topic: {KAFKA_TOPIC}")
    print("="*60)

    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='latest', 
            group_id='recsys_no_redis_group'
        )
    except Exception as e:
        print(f"❌ Kafka Error: {e}")
        return

    engine = RecommendationEngine()
    print("\n⏳ Đang chờ data...")
    
    for message in consumer:
        try:
            data = message.value
            user_id = data.get('user_id')
            item_val = data.get('item_id')
            ts = data.get('timestamp', '')

            if not user_id or not item_val: continue

            recs = engine.predict(user_id, item_val)

            ts_display = ts.split('T')[1][:8] if 'T' in ts else ts
            print("-" * 60)
            print(f"⏰ {ts_display} | 👤 {str(user_id)[:10]}... | 🖱️ CLICK: {item_val}")
            
            if recs:
                print(f"✨ AI Gợi ý: {recs}")
            else:
                print("⚠️  (Cold-start / Item mới)")

        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"⚠️ Error: {e}")

if __name__ == "__main__":
    main()