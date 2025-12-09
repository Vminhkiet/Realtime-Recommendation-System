import redis
import pickle
import numpy as np
import tensorflow as tf
import keras
import os

# --- CẤU HÌNH ---
# Tên service Redis trong docker-compose
REDIS_HOST = 'redis' 
REDIS_PORT = 6379

# Đường dẫn file trong Docker (Do bạn mount volume ./data -> /opt/data)
BASE_PATH = '/home/spark/work/data/model_registry'
MODEL_PATH = os.path.join(BASE_PATH, 'sasrec_v1.keras')
MAP_PATH = os.path.join(BASE_PATH, 'item_map.pkl')

MAX_LEN = 50  # Độ dài chuỗi lịch sử (Phải khớp lúc train)

class AIInferenceService:
    _model = None
    _item2id = None
    _id2item = None
    _redis = None

    @classmethod
    def get_redis(cls):
        if cls._redis is None:
            # Kết nối Redis để lưu Session
            cls._redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        return cls._redis

    @classmethod
    def load_resources(cls):
        """Load Model và Map 1 lần duy nhất (Singleton) để đỡ tốn RAM"""
        if cls._model is None:
            print("🔄 [AI Service] Đang load Model & Map...")
            try:
                # 1. Load Từ điển (Map)
                if not os.path.exists(MAP_PATH):
                    print(f"❌ Không tìm thấy file map tại: {MAP_PATH}")
                    return False

                with open(MAP_PATH, 'rb') as f:
                    # Load tuple (item2id, id2item)
                    cls._item2id, cls._id2item = pickle.load(f)
                
                # 2. Load Keras Model
                if not os.path.exists(MODEL_PATH):
                    print(f"❌ Không tìm thấy file model tại: {MODEL_PATH}")
                    return False

                # Import class SasRec để Keras hiểu (Trick quan trọng)
                from model import SasRec 
                cls._model = tf.keras.models.load_model(MODEL_PATH)
                print("✅ [AI Service] Model đã sẵn sàng!")
                return True
            except Exception as e:
                print(f"❌ Lỗi load resources: {e}")
                return False
        return True

    @classmethod
    def predict(cls, user_id, current_item_id):
        # Đảm bảo resource đã load
        if not cls.load_resources(): return []

        r = cls.get_redis()
        
        # 1. Cập nhật Redis (Sliding Window)
        # Thêm item mới vào lịch sử session của user
        key = f"history:{user_id}"
        r.rpush(key, current_item_id)
        r.ltrim(key, -MAX_LEN, -1) # Cắt, chỉ giữ 50 cái cuối cùng
        
        # 2. Lấy lịch sử ra để làm Input cho AI
        history_ids = r.lrange(key, 0, -1)
        
        # 3. Pre-processing (Chuỗi ID Amazon -> Chuỗi số nguyên)
        # Nếu item chưa có trong từ điển (hàng mới), dùng 0 (padding)
        seq_ints = [cls._item2id.get(item, 0) for item in history_ids]
        
        # Padding (Thêm số 0 vào trước cho đủ độ dài MAX_LEN)
        pad_len = MAX_LEN - len(seq_ints)
        input_seq = [0] * pad_len + seq_ints
        
        # Tạo Mask (True = có dữ liệu, False = padding)
        mask = [False] * pad_len + [True] * len(seq_ints)
        
        # 4. Inference (Gọi Model dự đoán)
        # Tạo tensor đúng định dạng model yêu cầu
        inputs = {
            "item_ids": tf.constant([input_seq]), 
            "padding_mask": tf.constant([mask])
        }
        
        try:
            # Model trả về Top-K ID (Dạng số)
            predictions = cls._model.predict(inputs, verbose=0)
            top_ids = predictions['predictions'][0] # Lấy batch đầu tiên
            
            # 5. Decode (Số -> Mã sản phẩm Amazon)
            rec_items = []
            for i in top_ids:
                i = int(i) # Convert numpy int to python int
                if i in cls._id2item:
                    rec_items.append(cls._id2item[i])
            
            return rec_items
        except Exception as e:
            print(f"⚠️ Lỗi khi predict: {e}")
            return []