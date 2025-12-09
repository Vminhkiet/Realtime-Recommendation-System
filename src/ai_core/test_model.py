import os
import pickle
import numpy as np
import tensorflow as tf
import keras
from model import SasRec  # Bắt buộc phải import class này để Keras hiểu model

# --- CẤU HÌNH ĐƯỜNG DẪN TUYỆT ĐỐI ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(BASE_DIR))

MODEL_PATH = os.path.join(PROJECT_ROOT, 'data/model_registry/sasrec_v1.keras')
MAP_PATH = os.path.join(PROJECT_ROOT, 'data/model_registry/item_map.pkl')
MAX_LEN = 50

def test_inference():
    print("🔄 Đang load tài nguyên...")
    
    # 1. Load Dictionary (Để dịch Số -> Tên sản phẩm)
    if not os.path.exists(MAP_PATH):
        print(f"❌ Lỗi: Không tìm thấy file map tại {MAP_PATH}")
        return

    with open(MAP_PATH, 'rb') as f:
        # Lúc tạo file data_process.py, ta đã lưu (item2id, id2item)
        item2id, id2item = pickle.load(f)
        
    print(f"✅ Đã load map. Tổng sản phẩm: {len(item2id)}")

    # 2. Load Model
    if not os.path.exists(MODEL_PATH):
        print(f"❌ Lỗi: Không tìm thấy model tại {MODEL_PATH}")
        return

    try:
        # Load model .keras (Keras tự động nhận diện class SasRec nhờ decorator @serializable)
        model = tf.keras.models.load_model(MODEL_PATH)
        print("✅ Đã load Model thành công!")
    except Exception as e:
        print(f"❌ Lỗi load model: {e}")
        return

    # 3. Giả lập Input (User vừa xem 3 món hàng)
    print("\n🧪 --- BẮT ĐẦU TEST DỰ ĐOÁN ---")
    
    # Lấy 3 món hàng bất kỳ có thật trong từ điển để test
    # (Lấy ID số 100, 101, 102 chẳng hạn)
    history_items = [100, 101, 102] 
    
    print(f"Input (User đã xem): {history_items}")
    print(f"Tên sản phẩm gốc: {[id2item.get(i, 'Unknown') for i in history_items]}")

    # 4. Tiền xử lý (Preprocessing) - Giống hệt lúc Train
    # Padding (Thêm số 0 vào sau cho đủ 50)
    pad_len = MAX_LEN - len(history_items)
    input_ids = history_items + [0] * pad_len
    
    # Masking (True cho item thật, False cho số 0)
    mask = [True] * len(history_items) + [False] * pad_len

    # Chuyển thành Tensor
    input_tensor = {
        "item_ids": tf.constant([input_ids]),       # Shape (1, 50)
        "padding_mask": tf.constant([mask])         # Shape (1, 50)
    }

    # 5. Gọi Model dự đoán
    # Model sẽ trả về dictionary chứa "predictions" (Top-K indices)
    output = model.predict(input_tensor, verbose=0)
    top_k_indices = output['predictions'][0] # Lấy kết quả của user đầu tiên

    # 6. Giải mã kết quả (Decoding)
    print("\n🎯 KẾT QUẢ GỢI Ý (TOP 10):")
    for rank, idx in enumerate(top_k_indices):
        item_id = int(idx)
        item_name = id2item.get(item_id, f"Unknown_ID_{item_id}")
        print(f"  #{rank+1}: {item_name} (ID: {item_id})")

if __name__ == "__main__":
    test_inference()