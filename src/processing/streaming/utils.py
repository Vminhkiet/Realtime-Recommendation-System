import redis
import pickle
import json # Load thêm file json map
import numpy as np
import tensorflow as tf
import os

# --- CẤU HÌNH ---
REDIS_HOST = 'redis'
REDIS_PORT = 6379
BASE_PATH = '/home/spark/work/data/model_registry'
MODEL_PATH = os.path.join(BASE_PATH, 'sasrec_v1.keras')
ITEM_MAP_PATH = os.path.join(BASE_PATH, 'item_map.json') # Sửa thành .json cho đồng bộ với Spark
CAT_MAP_PATH = os.path.join(BASE_PATH, 'category_map.json') # Cần thêm cái này

MAX_LEN = 50 

class AIInferenceService:
    _model = None
    _item_map = None # {id: label}
    _label_map = None # {label: id}
    _cat_map = None   # {item_label: cat_id} -> Cần mapping từ Item sang Category
    _redis = None

    @classmethod
    def get_redis(cls):
        if cls._redis is None:
            cls._redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        return cls._redis

    @classmethod
    def load_resources(cls):
        if cls._model is None:
            print("🔄 [AI Service] Loading resources...")
            try:
                # 1. Load Item Map (JSON)
                with open(ITEM_MAP_PATH, 'r') as f:
                    cls._item_map = json.load(f) # { "1": "B001...", "2": "B002..." }
                    # Tạo ngược lại để tra cứu: { "B001...": 1 }
                    cls._label_map = {v: int(k) for k, v in cls._item_map.items()}

                # 2. Load Category Map (Giả sử bạn có file map item->cat)
                # Nếu không có file mapping trực tiếp, ta có thể dùng heuristic hoặc load file metadata
                # Ở đây mình giả lập category = 1 (Unknown) để code chạy được đã
                # Trong thực tế bạn cần file: item_id -> category_id
                
                # 3. Load Model
                from model import SasRec
                cls._model = tf.keras.models.load_model(MODEL_PATH)
                print("✅ [AI Service] Ready!")
                return True
            except Exception as e:
                print(f"❌ Error loading resources: {e}")
                return False
        return True

    @classmethod
    def predict(cls, user_id, current_item_id):
        if not cls.load_resources(): return []
        r = cls.get_redis()
        
        # 1. Update Redis
        key = f"history:{user_id}"
        r.rpush(key, current_item_id)
        r.ltrim(key, -MAX_LEN, -1)
        
        history_labels = r.lrange(key, 0, -1)
        
        # 2. Prepare Inputs
        seq_ints = []
        cat_ints = []
        
        for label in history_labels:
            # Map Item Label -> Item ID (Int)
            item_idx = cls._label_map.get(label, 0)
            seq_ints.append(item_idx)
            
            # Map Item -> Category (Tạm thời để 1 nếu chưa có logic map)
            # TODO: Bạn cần logic lấy category đúng của item này
            cat_ints.append(1) 

        # Padding
        pad_len = MAX_LEN - len(seq_ints)
        input_ids = [0] * pad_len + seq_ints
        input_cats = [0] * pad_len + cat_ints # 🔥 Thêm dòng này
        mask = [False] * pad_len + [True] * len(seq_ints)

        # 3. Model Predict
        inputs = {
            "item_ids": tf.constant([input_ids]),
            "category_ids": tf.constant([input_cats]), # 🔥 Thêm dòng này
            "padding_mask": tf.constant([mask])
        }

        try:
            # Predict
            outputs = cls._model.predict(inputs, verbose=0)
            
            # Xử lý output (Check kỹ format trả về)
            if isinstance(outputs, dict) and "predictions" in outputs:
                top_ids = outputs["predictions"][0]
            else:
                # Fallback nếu Keras trả về array trực tiếp
                top_ids = outputs[0]

            # Decode (Int -> Label)
            rec_items = []
            for i in top_ids:
                idx = str(int(i)) # JSON key thường là string
                if idx in cls._item_map:
                    rec_items.append(cls._item_map[idx])
            
            return rec_items
        except Exception as e:
            print(f"⚠️ Prediction Error: {e}")
            return []