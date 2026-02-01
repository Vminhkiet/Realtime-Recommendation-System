import os
import sys
import numpy as np
import tensorflow as tf

# 1. THIẾT LẬP ĐƯỜNG DẪN
current_dir = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.abspath(os.path.join(current_dir, '../../'))
sys.path.append(current_dir) # Để tìm thấy file model.py cùng cấp

# Thêm Keras Hub vì trong file train bạn có nhắc đến các layer đặc thù
try:
    import keras_hub
except ImportError:
    print("⚠️ Keras Hub chưa cài, đang tiếp tục...")

try:
    from model import SasRec 
except ImportError:
    print("❌ Lỗi: Không tìm thấy lớp SasRec trong file src/ai_core/model.py!")
    sys.exit(1)

# Các đường dẫn từ file training của bạn
keras_model_path = os.path.join(BASE_DIR, 'data/model_registry/sasrec_v1.keras')
export_path = os.path.join(BASE_DIR, 'data/model_registry/1')

# 2. LOAD MÔ HÌNH KERAS
print(f"🔄 Đang nạp mô hình từ: {keras_model_path}")
# Phải có SasRec trong custom_objects để Keras định nghĩa lại được cấu hình
model = tf.keras.models.load_model(
    keras_model_path, 
    custom_objects={'SasRec': SasRec},
    compile=False # Không cần compile vì chỉ dùng để inference
)

# 3. MỒI DỮ LIỆU (DUMMY INPUT) - KHỚP VỚI CẤU HÌNH TRAINING
# Dựa trên file train: MAX_LEN = 50
print("🧪 Đang mồi dữ liệu giả lập để xác định Signatures...")
batch_size = 1
seq_len = 50

dummy_input = {
    "item_ids": np.zeros((batch_size, seq_len), dtype=np.int32),
    "category_ids": np.zeros((batch_size, seq_len), dtype=np.int32),
    "padding_mask": np.ones((batch_size, seq_len), dtype=np.bool_) # True cho padding
}

# Chạy thử để model xây dựng graph
_ = model(dummy_input)

# 4. XUẤT RA SAVEDMODEL (Định dạng C++ cho TF Serving)
print(f"🚀 Đang xuất SavedModel ra: {export_path}...")
if os.path.exists(export_path):
    import shutil
    shutil.rmtree(export_path)

# Sử dụng export() thay vì save() để tạo ra saved_model.pb
model.export(export_path)

print(f"✅ HOÀN THÀNH! TF Serving đã có thể đọc mô hình tại thư mục /1/")