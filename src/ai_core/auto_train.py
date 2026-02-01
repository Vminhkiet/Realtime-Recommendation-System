# import os
# import json
# import s3fs
# import random
# import pandas as pd
# import tensorflow as tf
# import numpy as np
# from datetime import datetime

# # Import Custom Layer (Bắt buộc)
# try:
#     from model import SasRec
# except ImportError:
#     from src.ai_core.model import SasRec

# # ================== CẤU HÌNH ==================
# MINIO_CONF = {
#     "key": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
#     "secret": os.getenv("MINIO_SECRET_KEY", "minioadmin"),
#     "client_kwargs": {"endpoint_url": os.getenv("MINIO_ENDPOINT", "http://minio:9000")}
# }

# BUCKET = "datalake"
# META_CONFIG_PATH = f"s3://{BUCKET}/model_registry/model_meta_config.json"
# MODEL_REGISTRY_S3 = f"s3://{BUCKET}/model_registry"
# LOCAL_MODEL_DIR = "/home/spark/work/data/models"  # Đường dẫn trong Docker

# # Hyperparameters
# LEARNING_RATE = 1e-4
# EPOCHS = 5  # Fine-tune thì số epoch thấp thôi
# BATCH_SIZE = 32
# MAX_LEN = 50

# # ================== HELPER FUNCTIONS ==================

# def get_fs():
#     return s3fs.S3FileSystem(**MINIO_CONF)

# def get_latest_config():
#     fs = get_fs()
#     if fs.exists(META_CONFIG_PATH):
#         with fs.open(META_CONFIG_PATH, 'r') as f:
#             return json.load(f)
#     return {}

# def update_meta_config(new_model_s3_path):
#     fs = get_fs()
#     config = get_latest_config()
    
#     # Cập nhật đường dẫn model mới nhất
#     config["latest_model_path"] = new_model_s3_path
#     config["last_trained_at"] = datetime.now().isoformat()
    
#     with fs.open(META_CONFIG_PATH, 'w') as f:
#         json.dump(config, f, indent=4)
#     print(f"✅ Config updated: Latest Model -> {new_model_s3_path}")

# def load_data_from_s3(s3_path):
#     print(f"📥 Loading Data: {s3_path}")
#     fs = get_fs()
#     # Chuyển s3a:// thành s3:// cho s3fs/pandas
#     path = s3_path.replace("s3a://", "s3://")
    
#     # Đọc Parquet từ S3
#     with fs.open(path, 'rb') as f:
#         df = pd.read_parquet(f)
    
#     print(f"📊 Loaded {len(df)} rows.")
#     return df

# def download_model_from_s3(s3_path, local_path):
#     fs = get_fs()
#     print(f"⬇️ Downloading model from {s3_path}...")
#     fs.get(s3_path, local_path, recursive=True)

# def upload_model_to_s3(local_path, s3_path):
#     fs = get_fs()
#     print(f"⬆️ Uploading model to {s3_path}...")
#     fs.put(local_path, s3_path, recursive=True)

# # Dataset Generator (Giữ nguyên logic của bạn)
# def create_dataset(item_seqs, cat_seqs, vocab_size):
#     # ... (Giữ nguyên code hàm generator của bạn) ...
#     # Lưu ý: Nhớ import create_dataset logic vào đây
#     pass 

# # ================== MAIN JOB ==================
# def main():
#     print("🚀 STARTING AUTO-TRAIN JOB...")
    
#     # 1. Đọc Config để biết lấy Data ở đâu và Model cũ ở đâu
#     config = get_latest_config()
#     data_path = config.get("incremental_path")  # Lấy path tuần này
#     latest_model_s3 = config.get("latest_model_path") # Lấy model tuần trước
#     vocab_size = config.get("max_item_idx", 10000) # Lấy vocab size từ ETL

#     if not data_path:
#         print("❌ Không tìm thấy 'incremental_path' trong config. Hủy train.")
#         return

#     # 2. Load Data
#     try:
#         df = load_data_from_s3(data_path)
#         item_seqs = df['sequence_ids'].tolist()
#         cat_seqs = df['category_ids'].tolist()
#     except Exception as e:
#         print(f"⚠️ Lỗi load data: {e}"); return

#     # 3. Chuẩn bị Model (Download hoặc Load Base)
#     os.makedirs(LOCAL_MODEL_DIR, exist_ok=True)
#     local_model_path = os.path.join(LOCAL_MODEL_DIR, "current_model.keras")
    
#     if latest_model_s3:
#         # Nếu đã có model trên S3, tải về để train tiếp
#         download_model_from_s3(latest_model_s3, local_model_path)
#         print("🔄 Loading Existing Model for Fine-tuning...")
#         model = tf.keras.models.load_model(local_model_path, custom_objects={'SasRec': SasRec}, compile=False)
#     else:
#         print("🆕 Creating NEW Model (First Run)...")
#         # Logic khởi tạo model mới nếu chưa có (cần import class SasRec và khởi tạo)
#         # model = SasRec(...) 
#         # model.build(...)
#         print("⚠️ Chưa có model gốc. Hãy train Base Model trước!"); return

#     # 4. Compile & Train
#     # Logic dataset (cần copy hàm create_dataset vào)
#     train_ds = create_dataset(item_seqs, cat_seqs, vocab_size) 
    
#     model.compile(optimizer=tf.keras.optimizers.Adam(learning_rate=LEARNING_RATE))
#     model.fit(train_ds, epochs=EPOCHS)

#     # 5. Lưu & Upload Model Mới
#     # Tạo tên version theo ngày: sasrec_20251229.keras
#     version = datetime.now().strftime("%Y%m%d_%H%M")
#     new_model_name = f"sasrec_{version}.keras"
    
#     local_save_path = os.path.join(LOCAL_MODEL_DIR, new_model_name)
#     model.save(local_save_path)
#     print(f"💾 Saved local: {local_save_path}")
    
#     s3_save_path = f"{MODEL_REGISTRY_S3}/{new_model_name}"
#     upload_model_to_s3(local_save_path, s3_save_path)
    
#     # 6. Cập nhật Config để tuần sau biết đường dùng
#     update_meta_config(s3_save_path)
    
#     print("🎉 FINE-TUNING COMPLETE.")

# if __name__ == "__main__":
#     main()


# import os
# import json
# import s3fs
# import random
# import pandas as pd
# import tensorflow as tf
# import numpy as np
# from datetime import datetime
# import mlflow
# import mlflow.tensorflow

# MLFLOW_TRACKING_URI = os.getenv(
#     "MLFLOW_TRACKING_URI",
#     "http://mlflow:5000"
# )

# mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
# mlflow.set_experiment("sasrec_weekly_training")

# # --- IMPORT MODEL CLASS ---
# try:
#     from model import SasRec
# except ImportError:
#     import sys
#     sys.path.append(os.getcwd())
#     from src.ai_core.model import SasRec

# # ================== 1. CẤU HÌNH WEEKLY (NHẸ NHÀNG) ==================
# MINIO_CONF = {
#     "key": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
#     "secret": os.getenv("MINIO_SECRET_KEY", "minioadmin"),
#     "client_kwargs": {"endpoint_url": os.getenv("MINIO_ENDPOINT", "http://minio:9000")}
# }

# BUCKET = "datalake"
# META_CONFIG_PATH = f"s3://{BUCKET}/model_registry/model_meta_config.json"
# MODEL_REGISTRY_S3 = f"s3://{BUCKET}/model_registry"
# LOCAL_MODEL_DIR = "/home/spark/work/data/models"

# # --- HYPERPARAMETERS ---
# MAX_LEN = 10       # Khớp với ETL Spark
# BATCH_SIZE = 64
# EPOCHS = 5         # Số epoch ít vì chỉ cần học data tuần này
# LEARNING_RATE = 5e-5 # CỰC KỲ QUAN TRỌNG: LR nhỏ để Fine-tune

# # ================== 2. HELPER FUNCTIONS ==================

# def get_fs():
#     return s3fs.S3FileSystem(**MINIO_CONF)

# def get_latest_config():
#     fs = get_fs()
#     if fs.exists(META_CONFIG_PATH):
#         with fs.open(META_CONFIG_PATH, 'r') as f:
#             return json.load(f)
#     return {}

# def update_meta_config(new_model_s3_path, metrics=None):
#     fs = get_fs()
#     config = get_latest_config()
    
#     config["latest_model_path"] = new_model_s3_path
#     config["last_trained_at"] = datetime.now().isoformat()
#     config["train_mode"] = "WEEKLY_INCREMENTAL"

#     if metrics:
#         if "history" not in config: config["history"] = []
#         record = metrics.copy()
#         record["date"] = config["last_trained_at"]
#         record["model"] = new_model_s3_path.split("/")[-1]
#         config["history"].append(record)
#         if len(config["history"]) > 20: config["history"] = config["history"][-20:]

#     with fs.open(META_CONFIG_PATH, 'w') as f:
#         json.dump(config, f, indent=4)
#     print(f"✅ [Config] Updated Latest Model -> {new_model_s3_path}")

# def load_data_from_s3(s3_path):
#     path = s3_path.replace("s3a://", "s3://")
#     print(f"📥 Loading Data: {path}")
#     fs = get_fs()
#     if not fs.exists(path):
#         print(f"⚠️ Path not found: {path}")
#         return pd.DataFrame()
#     return pd.read_parquet(path, storage_options=MINIO_CONF)

# def create_dataset(item_seqs, cat_seqs, vocab_size):
#     """Dataset Generator cho Weekly Data"""
#     def generator():
#         for items, cats in zip(item_seqs, cat_seqs):
#             in_items = items[:-1]
#             target_items = items[1:]
#             in_cats = cats[:-1]
            
#             # Logic Padding (Cho user mới) & Cắt (Cho user cũ)
#             curr_len = len(in_items)
#             pad_len = MAX_LEN - curr_len
            
#             if pad_len < 0:
#                 in_items = in_items[-MAX_LEN:]
#                 target_items = target_items[-MAX_LEN:]
#                 in_cats = in_cats[-MAX_LEN:]
#                 pad_len = 0
#                 curr_len = MAX_LEN

#             padded_items = list(in_items) + [0] * pad_len
#             padded_cats = list(in_cats) + [0] * pad_len
#             padded_targets = list(target_items) + [0] * pad_len
#             padding_mask = [True] * curr_len + [False] * pad_len
            
#             # Negative Sampling
#             neg_items = [random.randint(1, vocab_size - 1) for _ in range(MAX_LEN)]

#             yield (
#                 {
#                     "item_ids": np.array(padded_items, dtype=np.int32),
#                     "category_ids": np.array(padded_cats, dtype=np.int32),
#                     "padding_mask": np.array(padding_mask, dtype=np.bool_)
#                 },
#                 {
#                     "positive_sequence": np.array(padded_targets, dtype=np.int32),
#                     "negative_sequence": np.array(neg_items, dtype=np.int32)
#                 }
#             )

#     return tf.data.Dataset.from_generator(
#         generator,
#         output_signature=(
#             {
#                 "item_ids": tf.TensorSpec((MAX_LEN,), tf.int32),
#                 "category_ids": tf.TensorSpec((MAX_LEN,), tf.int32),
#                 "padding_mask": tf.TensorSpec((MAX_LEN,), tf.bool),
#             },
#             {
#                 "positive_sequence": tf.TensorSpec((MAX_LEN,), tf.int32),
#                 "negative_sequence": tf.TensorSpec((MAX_LEN,), tf.int32),
#             }
#         )
#     ).batch(BATCH_SIZE).prefetch(tf.data.AUTOTUNE)

# # ================== 3. MAIN WEEKLY JOB ==================
# def main():
#     print(f"🚀 STARTING WEEKLY FINE-TUNING (MAX_LEN={MAX_LEN})...")
    
#     # 1. Kiểm tra Config
#     config = get_latest_config()
#     train_path = config.get("train_path")
#     test_path = config.get("test_path")
#     latest_model_s3 = config.get("latest_model_path")
#     data_max_idx = config.get("max_item_idx", 0)

#     if not latest_model_s3:
#         print("❌ CRITICAL: Không tìm thấy model gốc (Base Model).")
#         print("👉 Vui lòng chạy script 'train_cold_start.py' trước để tạo model nền tảng.")
#         return

#     if not train_path:
#         print("❌ Thiếu path data tuần này. Hủy job.")
#         return

#     # 2. Load Data Tuần
#     df_train = load_data_from_s3(train_path)
#     if df_train.empty:
#         print("⚠️ Data tuần này rỗng. Không có gì để học.")
#         return
#     print(f"📊 New Data Rows: {len(df_train)}")

#     # 3. Load Model Cũ
#     os.makedirs(LOCAL_MODEL_DIR, exist_ok=True)
#     local_model_path = os.path.join(LOCAL_MODEL_DIR, "base_model.keras")
#     fs = get_fs()

#     print(f"🔄 Downloading Base Model: {latest_model_s3}")
#     try:
#         fs.get(latest_model_s3.replace("s3a://", "s3://"), local_model_path)
#         model = tf.keras.models.load_model(local_model_path, custom_objects={'SasRec': SasRec}, compile=False)
#     except Exception as e:
#         print(f"❌ Lỗi load model cũ: {e}")
#         return

#     # 4. SAFETY CHECK (Quan trọng nhất của Weekly Train)
#     # Kiểm tra xem Model cũ có đủ chỗ chứa Item mới không
#     current_vocab_capacity = config.get("max_item_idx") + 1
#     print(f"ℹ️ Model Capacity: {current_vocab_capacity} items | Max New Item ID: {data_max_idx}")

#     if data_max_idx >= current_vocab_capacity:
#         print("\n" + "="*50)
#         print("🛑 STOPPING JOB: TRÀN BỘ NHỚ ITEM (VOCAB OVERFLOW)")
#         print(f"   - Model cũ chỉ hỗ trợ ID đến: {current_vocab_capacity}")
#         print(f"   - Data mới xuất hiện ID: {data_max_idx}")
#         print("👉 GIẢI PHÁP: Hãy chạy lại 'train_cold_start.py' để Resize lại Embedding Matrix.")
#         print("="*50 + "\n")
#         return # Dừng luôn, không cố train vì sẽ crash

#     # 5. Prepare Dataset
#     # Lưu ý: Dùng current_vocab_capacity để làm vocab_size cho generator
#     train_ds = create_dataset(df_train['sequence_ids'], df_train['category_ids'], current_vocab_capacity)

#     # 6. Fine-tuning
#     # print(f"🔥 Fine-tuning start (LR: {LEARNING_RATE} - Epochs: {EPOCHS})...")
    
#     # model.compile(optimizer=tf.keras.optimizers.Adam(learning_rate=LEARNING_RATE), loss=None, metrics=['accuracy'])
#     # history = model.fit(train_ds, epochs=EPOCHS)

#     # # 7. Evaluate
#     # metrics = {"loss": history.history['loss'][-1], "accuracy": history.history['accuracy'][-1]}

#     import mlflow
#     import mlflow.tensorflow

#     mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
#     mlflow.set_experiment("sasrec_weekly_training")

#     print(f"🔥 Fine-tuning start (LR: {LEARNING_RATE} - Epochs: {EPOCHS})...")

#     with mlflow.start_run(run_name=f"weekly_{datetime.now().strftime('%Y%m%d_%H%M')}"):

#         # ===== LOG PARAMS =====
#         mlflow.log_param("max_len", MAX_LEN)
#         mlflow.log_param("batch_size", BATCH_SIZE)
#         mlflow.log_param("epochs", EPOCHS)
#         mlflow.log_param("learning_rate", LEARNING_RATE)
#         mlflow.log_param("train_mode", "weekly_incremental")

#         model.compile(
#             optimizer=tf.keras.optimizers.Adam(learning_rate=LEARNING_RATE)
#         )

#         history = model.fit(train_ds, epochs=EPOCHS)

#         # ===== TRAIN METRICS =====
#         train_loss = history.history["loss"][-1]

#         mlflow.log_metric("train_loss", train_loss)

#         metrics = {
#             "loss": train_loss
#         }

#         # ===== EVALUATE =====
#         if test_path:
#             try:
#                 df_test = load_data_from_s3(test_path)
#                 if not df_test.empty:
#                     test_ds = create_dataset(
#                         df_test['sequence_ids'],
#                         df_test['category_ids'],
#                         current_vocab_capacity
#                     )
#                     eval_res = model.evaluate(test_ds, return_dict=True)
#                     for k, v in eval_res.items():
#                         mlflow.log_metric(f"test_{k}", v)
#                     metrics.update(eval_res)
#             except Exception as e:
#                 print(f"⚠️ Eval skipped: {e}")

#         # ===== LOG MODEL TO MLFLOW =====
#         timestamp = datetime.now().strftime("%Y%m%d_%H%M")
#         MODEL_DIR = f"/tmp/sasrec_weekly_{timestamp}.keras"
#         os.makedirs(os.path.dirname(MODEL_DIR), exist_ok=True)

#         model.save(MODEL_DIR)
#         mlflow.log_artifacts(MODEL_DIR, artifact_path="model")



#     if test_path:
#         try:
#             df_test = load_data_from_s3(test_path)
#             if not df_test.empty:
#                 test_ds = create_dataset(df_test['sequence_ids'], df_test['category_ids'], current_vocab_capacity)
#                 eval_res = model.evaluate(test_ds, return_dict=True)
#                 metrics.update(eval_res)
#                 print(f"📊 Test Metrics: {metrics}")
#         except: pass

#     # 8. Save Version Mới
#     timestamp = datetime.now().strftime("%Y%m%d_%H%M")
#     new_model_name = f"sasrec_weekly_{timestamp}.keras"
#     local_save_path = os.path.join(LOCAL_MODEL_DIR, new_model_name)
    
#     print(f"💾 Saving Version: {new_model_name}")
#     model.save(local_save_path)
    
#     s3_save_path = f"s3://{BUCKET}/model_registry/{new_model_name}"
#     print(f"⬆️ Uploading to S3...")
#     fs.put(local_save_path, s3_save_path)

#     # 9. Update Config
#     update_meta_config(s3_save_path, metrics)
#     print("🎉 WEEKLY UPDATE COMPLETE.")

# if __name__ == "__main__":
#     main()
# import os
# import sys
# import json
# import random
# import pandas as pd
# import tensorflow as tf
# import numpy as np
# import mlflow
# import mlflow.tensorflow
# import boto3
# from collections import Counter
# from datetime import datetime

# # ==========================================
# # 1. SETUP CREDENTIALS
# # ==========================================
# os.environ["AWS_ACCESS_KEY_ID"] = "minioadmin"
# os.environ["AWS_SECRET_ACCESS_KEY"] = "minioadmin"
# os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
# MINIO_URL = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
# os.environ["MLFLOW_S3_ENDPOINT_URL"] = MINIO_URL
# os.environ["AWS_ENDPOINT_URL"] = MINIO_URL
# os.environ["AWS_S3_ENDPOINT_URL"] = MINIO_URL
# os.environ["MLFLOW_TRACKING_URI"] = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")

# # ==========================================
# # 2. IMPORTS & CONFIG
# # ==========================================
# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# try:
#     from model import SasRec
# except ImportError:
#     try:
#         from src.ai_core.model import SasRec
#     except ImportError:
#         print("❌ CRITICAL: Cannot import SasRec model class.")
#         sys.exit(1)

# BUCKET_NAME = "datalake"
# META_CONFIG_KEY = "model_registry/model_meta_config.json"
# MAP_PATH = f"s3a://{BUCKET_NAME}/model_registry/item_map.json"

# # Local temp paths
# LOCAL_MODEL_PATH = "/tmp/sasrec_daily.keras"
# LOCAL_PREV_MODEL = "data/model_registry/sasrec_v1.keras"

# # Hyperparameters
# MAX_USERS = 20000
# MIN_INTERACTIONS = 5
# MAX_LEN = 50
# EMBED_DIM = 64
# NUM_BLOCKS = 2
# NUM_HEADS = 4
# DROPOUT_RATE = 0.1
# BATCH_SIZE = 64
# EPOCHS = 5      
# LEARNING_RATE = 5e-5

# # ==========================================
# # 3. HELPER FUNCTIONS (MINIO)
# # ==========================================
# def get_s3_client():
#     return boto3.client("s3", endpoint_url=MINIO_URL,
#                         aws_access_key_id="minioadmin",
#                         aws_secret_access_key="minioadmin")

# def parse_s3_path(s3_path):
#     """Chuyển s3a://bucket/key thành (bucket, key)"""
#     clean_path = s3_path.replace("s3a://", "").replace("s3://", "")
#     parts = clean_path.split("/", 1)
#     return parts[0], parts[1]

# def get_meta_config():
#     """Đọc file config từ MinIO"""
#     s3 = get_s3_client()
#     try:
#         obj = s3.get_object(Bucket=BUCKET_NAME, Key=META_CONFIG_KEY)
#         return json.loads(obj['Body'].read().decode('utf-8'))
#     except Exception as e:
#         print(f"⚠️ Không đọc được Config ({e}). Sẽ dùng tham số mặc định.")
#         return {}

# def update_meta_config(new_model_path):
#     """Cập nhật đường dẫn model mới vào Config trên MinIO"""
#     s3 = get_s3_client()
#     try:
#         # 1. Đọc lại config mới nhất
#         current_config = get_meta_config()
        
#         # 2. Update trường latest_model_path
#         current_config["latest_model_path"] = new_model_path
#         current_config["last_trained_at"] = datetime.now().isoformat()
        
#         # 3. Ghi đè lại lên MinIO
#         s3.put_object(
#             Bucket=BUCKET_NAME, 
#             Key=META_CONFIG_KEY, 
#             Body=json.dumps(current_config, indent=4),
#             ContentType='application/json'
#         )
#         print(f"✅ [METADATA] Đã cập nhật latest_model_path: {new_model_path}")
#     except Exception as e:
#         print(f"❌ [METADATA] Lỗi cập nhật Config: {e}")

# # ==========================================
# # 4. DATA LOADING (Updated to use Config Paths)
# # ==========================================
# def load_data(train_path_s3):
#     print(f"📥 Loading Training Data from: {train_path_s3}")
    
#     # Pandas read_parquet hỗ trợ s3 nếu cài s3fs, nhưng ta dùng boto3 tải về cho chắc
#     local_parquet = "/tmp/train_data.parquet"
#     bucket, key = parse_s3_path(train_path_s3)
    
#     s3 = get_s3_client()
#     # Logic download folder parquet (Spark lưu folder)
#     # Để đơn giản, giả sử pandas đọc thẳng qua s3 storage options
#     try:
#         df = pd.read_parquet(
#             train_path_s3.replace("s3a://", "s3://"),
#             storage_options={
#                 "key": "minioadmin", "secret": "minioadmin", 
#                 "client_kwargs": {"endpoint_url": MINIO_URL}
#             }
#         )
#     except:
#         print("⚠️ Lỗi đọc S3 trực tiếp, kiểm tra lại path.")
#         return [], [], [], 50000, 100

#     # Xử lý Timestamp
#     time_col = 'last_ts' if 'last_ts' in df.columns else 'last_timestamp'
#     df['last_timestamp'] = df[time_col].apply(lambda x: x/1000 if x > 32503680000 else x)

#     # Lấy dữ liệu
#     item_seqs = df['sequence_ids'].tolist()
#     cat_seqs = df['category_ids'].tolist()
    
#     # Load Vocab Size từ Map
#     try:
#         map_bucket, map_key = parse_s3_path(MAP_PATH)
#         obj = s3.get_object(Bucket=map_bucket, Key=map_key)
#         vocab_size = len(json.loads(obj['Body'].read().decode('utf-8')))
#     except:
#         vocab_size = 106000 # Fallback

#     return item_seqs, cat_seqs, vocab_size, 100

# # ==========================================
# # 5. DATASET GENERATOR
# # ==========================================
# def create_dataset(item_seqs, cat_seqs, max_len, num_items, is_training=True):
#     def generator():
#         data = list(zip(item_seqs, cat_seqs))
#         if is_training: random.shuffle(data)
#         for item_seq, cat_seq in data:
#             # Code cũ của bạn...
#             # Đơn giản hóa cho ngắn gọn (Logic sliding window giữ nguyên)
#             # Giả sử seq đã được cắt sẵn từ ETL (ETL bạn làm khá kỹ rồi)
            
#             # Pad sequence
#             seq_len = len(item_seq)
#             pad_len = max_len - seq_len
            
#             if pad_len > 0:
#                 input_ids = list(item_seq) + [0] * pad_len
#                 cat_ids = list(cat_seq) + [0] * pad_len
#                 mask = [True] * seq_len + [False] * pad_len
#             else:
#                 input_ids = list(item_seq)[-max_len:]
#                 cat_ids = list(cat_seq)[-max_len:]
#                 mask = [True] * max_len
            
#             # Negative Sampling đơn giản
#             pos_ids = input_ids[1:] + [0] # Shift left
#             neg_ids = [random.randint(1, num_items) for _ in range(max_len)]
            
#             yield (
#                 {"item_ids": input_ids, "category_ids": cat_ids, "padding_mask": mask},
#                 {"positive_sequence": pos_ids, "negative_sequence": neg_ids}
#             )

#     return tf.data.Dataset.from_generator(
#         generator,
#         output_signature=(
#             {"item_ids": tf.TensorSpec((max_len,), tf.int32), "category_ids": tf.TensorSpec((max_len,), tf.int32), "padding_mask": tf.TensorSpec((max_len,), tf.bool)},
#             {"positive_sequence": tf.TensorSpec((max_len,), tf.int32), "negative_sequence": tf.TensorSpec((max_len,), tf.int32)}
#         )
#     ).batch(BATCH_SIZE).prefetch(tf.data.AUTOTUNE)

# # ==========================================
# # 6. MAIN PIPELINE
# # ==========================================
# def main():
#     print(f"🚀 DAILY TRAIN STARTED | URI: {os.environ['MLFLOW_TRACKING_URI']}")
    
#     # 1. ĐỌC CONFIG TỪ MINIO
#     config = get_meta_config()
#     if not config:
#         print("❌ CRITICAL: Không lấy được Config. Dừng.")
#         return

#     # Lấy các đường dẫn quan trọng
#     latest_model_s3 = config.get("latest_model_path")
#     train_data_s3 = config.get("train_path") # Lấy path data mới nhất từ ETL hôm nay
    
#     print(f"📋 Config Loaded:")
#     print(f"   - Train Data: {train_data_s3}")
#     print(f"   - Prev Model: {latest_model_s3}")

#     # 2. LOAD DATA
#     item_seqs, cat_seqs, vocab_size, num_categories = load_data(train_data_s3)
    
#     # Popular items (Dummy logic cho nhanh)
#     popular_items = [i for i in range(1, 1000)] 

#     # Split 90/10
#     val_size = max(1, int(len(item_seqs) * 0.1))
#     train_ds = create_dataset(item_seqs[val_size:], cat_seqs[val_size:], MAX_LEN, vocab_size, is_training=True)
#     val_ds = create_dataset(item_seqs[:val_size], cat_seqs[:val_size], MAX_LEN, vocab_size, is_training=False)

#     # 3. MLFLOW RUN
#     mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])
#     mlflow.set_experiment("sasrec_daily_update")
    
#     with mlflow.start_run() as run:
#         # Build Model
#         model = SasRec(
#             vocabulary_size=vocab_size + 1,
#             category_size=num_categories + 1,
#             num_layers=NUM_BLOCKS, num_heads=NUM_HEADS,
#             hidden_dim=EMBED_DIM, dropout=DROPOUT_RATE,
#             max_sequence_length=MAX_LEN
#         )
#         # Dummy call to build variables
#         model({"item_ids": tf.zeros((1, MAX_LEN), dtype=tf.int32), "category_ids": tf.zeros((1, MAX_LEN), dtype=tf.int32), "padding_mask": tf.zeros((1, MAX_LEN), dtype=tf.bool)})

#         # ---------------------------------------------------------
#         # 🔥 LOAD MODEL CŨ TỪ MINIO (INCREMENTAL)
#         # ---------------------------------------------------------
#         if latest_model_s3 and "sasrec" in latest_model_s3:
#             print(f"⬇️ Downloading previous model from: {latest_model_s3}")
#             try:
#                 print(latest_model_s3)
#                 bucket, key = parse_s3_path(latest_model_s3)
#                 print(latest_model_s3)
#                 s3 = get_s3_client()
#                 s3.download_file(bucket, key, LOCAL_PREV_MODEL)
#                 print("11")
#                 model.load_weights(LOCAL_PREV_MODEL)
#                 print("✅ [INCREMENTAL] Loaded weights successfully.")
#             except Exception as e:
#                 print(f"⚠️ Failed to load previous model ({e}). Training from scratch.")
#         else:
#             print("⚠️ No previous model path in config. Training from scratch.")

#         # ---------------------------------------------------------
#         # TRAIN
#         # ---------------------------------------------------------
#         model.compile(optimizer=tf.keras.optimizers.Adam(learning_rate=LEARNING_RATE))
        
#         print(f"🔥 Fine-tuning for {EPOCHS} epochs...")
#         model.fit(train_ds, validation_data=val_ds, epochs=EPOCHS)

#         # ---------------------------------------------------------
#         # SAVE & UPDATE CONFIG
#         # ---------------------------------------------------------
#         print("💾 Saving model locally...")
#         model.save(LOCAL_MODEL_PATH)
        
#         print("⬆️ Uploading to MLflow...")
#         mlflow.log_artifact(LOCAL_MODEL_PATH, artifact_path="model_keras")
        
#         # Lấy S3 URI từ MLflow Run hiện tại
#         # MLflow thường lưu dạng: s3://mlflow/run_id/artifacts/model_keras/sasrec_daily.keras
#         artifact_uri = mlflow.get_artifact_uri("model_keras/sasrec_daily.keras")
        
#         # Chuyển đổi sang s3a:// để Spark dùng được (nếu cần) hoặc giữ s3://
#         # Thống nhất dùng s3a:// cho hệ sinh thái Hadoop/Spark
#         new_s3_path = artifact_uri.replace("s3://", "s3a://")
        
#         print(f"🔗 New Model Path: {new_s3_path}")
        
#         # CẬP NHẬT JSON CONFIG TRÊN MINIO
#         update_meta_config(new_s3_path)

#     print("🎉 DAILY UPDATE COMPLETE.")

# if __name__ == "__main__":
#     main()
import os
import sys
import json
import random
import pickle
import pandas as pd
import tensorflow as tf
import numpy as np
import mlflow
import mlflow.tensorflow
import boto3
from collections import Counter
from datetime import datetime

# ==========================================
# 1. SETUP CREDENTIALS & ENV
# ==========================================
os.environ["AWS_ACCESS_KEY_ID"] = "minioadmin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "minioadmin"
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
MINIO_URL = os.getenv("MINIO_ENDPOINT", "http://minio:9000")

# Cấu hình MLflow & S3
os.environ["MLFLOW_S3_ENDPOINT_URL"] = MINIO_URL
os.environ["AWS_ENDPOINT_URL"] = MINIO_URL
os.environ["AWS_S3_ENDPOINT_URL"] = MINIO_URL
os.environ["MLFLOW_TRACKING_URI"] = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")

# Bật Mixed Precision (Tùy chọn)
try:
    if tf.config.list_physical_devices('GPU'):
        tf.keras.mixed_precision.set_global_policy('mixed_float16')
        print("⚡ Mixed Precision Enabled (Float16).")
except: pass

# ==========================================
# 2. IMPORTS & CONFIG
# ==========================================
# Import SasRec Model
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from model import SasRec
except ImportError:
    try:
        from src.ai_core.model import SasRec
    except ImportError:
        print("❌ CRITICAL: Cannot import SasRec model class.")
        sys.exit(1)

# Paths & Constants
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(BASE_DIR))

# Input Paths
PARQUET_PATH = os.path.join(PROJECT_ROOT, 'data/model_registry/processed_parquet')
MAP_PATH = os.path.join(PROJECT_ROOT, 'data/model_registry/item_map.json')
CAT_MAP_PATH = os.path.join(PROJECT_ROOT, 'data/model_registry/category_map.json')

# Output Paths
MODEL_SAVE_PATH = "data/model_registry/sasrec_v1.keras"
TEST_SET_SAVE_PATH = "data/model_registry/test_set.pkl"
LOG_PATH = "data/model_registry/training_history.json"
VALID_USERS_PATH = os.path.join(PROJECT_ROOT, 'src/simulation/valid_users.json')

# Hyperparameters (Fine-tuning settings)
MAX_USERS = 20000       # Lấy 20k user tốt nhất để fine-tune
MAX_LEN = 50
BATCH_SIZE = 64
EPOCHS = 5              # Số epoch thấp cho retrain
STEPS_PER_EPOCH = 200   
STEP_SIZE = 10          
FINE_TUNE_LR = 0.0001   # Learning rate nhỏ hơn bình thường (1e-4)

# ==========================================
# 3. UTILS
# ==========================================
def ensure_bucket_exists(bucket_name="mlflow"):
    s3 = boto3.client("s3", endpoint_url=MINIO_URL)
    try: s3.head_bucket(Bucket=bucket_name)
    except: 
        try: s3.create_bucket(Bucket=bucket_name)
        except: pass

def normalize_ts(ts):
    return ts / 1000 if ts > 32503680000 else ts

def get_popular_items(sequences, top_k=2000):
    print("🔥 Đang tính Popular Items (để làm Hard Negatives)...")
    all_items = [i for seq in sequences for i in seq]
    counter = Counter(all_items)
    if 0 in counter: del counter[0]
    return [item for item, _ in counter.most_common(top_k)]

# ==========================================
# 4. LOAD DATA
# ==========================================
def load_data():
    print("📥 Đang load dữ liệu Parquet...")
    if not os.path.exists(PARQUET_PATH):
        raise FileNotFoundError(f"❌ Không tìm thấy file: {PARQUET_PATH}")

    df = pd.read_parquet(PARQUET_PATH)
    
    # Xử lý Timestamp
    if 'last_timestamp' in df.columns:
        TIME_COL = 'last_timestamp'
    elif 'sequence_timestamps' in df.columns:
        df['last_timestamp'] = df['sequence_timestamps'].apply(lambda x: x[-1] if len(x) > 0 else 0)
        TIME_COL = 'last_timestamp'
    else:
        df['last_timestamp'] = 0
        TIME_COL = 'last_timestamp'

    df[TIME_COL] = df[TIME_COL].apply(normalize_ts)
    if 'user_id' not in df.columns: df['user_id'] = df.index.astype(str)

    # Lọc User tốt nhất
    print(f"🔍 Sàng lọc Top {MAX_USERS} Users có lịch sử dày nhất...")
    df['seq_len'] = df['sequence_ids'].apply(len)
    df_sorted = df.sort_values(by='seq_len', ascending=False).head(MAX_USERS)
    df_final = df_sorted.sample(frac=1).reset_index(drop=True)

    print(f"✅ Đã chọn: {len(df_final)} users.")

    item_seqs = df_final['sequence_ids'].tolist()
    cat_seqs = df_final['category_ids'].tolist()
    last_times = df_final[TIME_COL].tolist()
    valid_user_ids = df_final['user_id'].tolist()

    # Save Metadata
    os.makedirs(os.path.dirname(VALID_USERS_PATH), exist_ok=True)
    with open(VALID_USERS_PATH, 'w') as f:
        json.dump(valid_user_ids, f)

    with open(MAP_PATH, 'r') as f:
        vocab_size = len(json.load(f))
    
    if os.path.exists(CAT_MAP_PATH):
        with open(CAT_MAP_PATH, 'r') as f:
            num_categories = len(json.load(f))
    else:
        num_categories = 100

    return item_seqs, cat_seqs, last_times, vocab_size, num_categories

# ==========================================
# 5. DATASET GENERATOR
# ==========================================
def create_dataset(item_seqs, cat_seqs, max_len, num_items, popular_items, is_training=True):
    def generator():
        data = list(zip(item_seqs, cat_seqs))
        if is_training: random.shuffle(data)
        
        for item_seq, cat_seq in data:
            item_seq = list(item_seq)
            cat_seq = list(cat_seq)
            seq_len = len(item_seq)

            if is_training:
                if seq_len <= max_len + 1: starts = [0]
                else: starts = range(0, seq_len - max_len, STEP_SIZE)
            else:
                starts = [max(0, seq_len - max_len - 1)]

            for i in starts:
                end = min(i + max_len + 1, seq_len)
                item_win = item_seq[i:end]
                cat_win = cat_seq[i:end]
                
                if len(item_win) < 2: continue

                curr_item = item_win[:-1]
                curr_pos = item_win[1:]
                curr_cat = cat_win[:-1]

                # Cắt đuôi
                curr_item = curr_item[-max_len:]
                curr_pos = curr_pos[-max_len:]
                curr_cat = curr_cat[-max_len:]

                # Padding
                input_ids = list(curr_item)
                pos_ids = list(curr_pos)
                cat_ids = list(curr_cat)
                pad_len = max_len - len(input_ids)

                input_ids = input_ids + [0] * pad_len
                pos_ids = pos_ids + [0] * pad_len
                cat_ids = cat_ids + [0] * pad_len
                mask = [True] * len(curr_item) + [False] * pad_len

                # Negative Sampling
                win_set = set(item_win)
                neg_ids = []
                for _ in range(len(curr_item)):
                    if is_training and random.random() < 0.3 and popular_items:
                        neg = random.choice(popular_items)
                    else:
                        neg = random.randint(1, num_items)
                    while neg in win_set:
                        neg = random.randint(1, num_items)
                    neg_ids.append(neg)
                
                neg_ids += [0] * pad_len

                yield (
                    {"item_ids": input_ids, "category_ids": cat_ids, "padding_mask": mask},
                    {"positive_sequence": pos_ids, "negative_sequence": neg_ids}
                )

    ds = tf.data.Dataset.from_generator(
        generator,
        output_signature=(
            {"item_ids": tf.TensorSpec((max_len,), tf.int32), "category_ids": tf.TensorSpec((max_len,), tf.int32), "padding_mask": tf.TensorSpec((max_len,), tf.bool)},
            {"positive_sequence": tf.TensorSpec((max_len,), tf.int32), "negative_sequence": tf.TensorSpec((max_len,), tf.int32)}
        )
    )
    
    if is_training:
        ds = ds.repeat().batch(BATCH_SIZE).prefetch(tf.data.AUTOTUNE)
    else:
        ds = ds.batch(BATCH_SIZE).prefetch(tf.data.AUTOTUNE)
    
    return ds

# ==========================================
# 6. MAIN FUNCTION (STRICT RETRAIN ONLY)
# ==========================================
def main():
    print("🚀 STRICT RETRAINING PIPELINE STARTED")
    ensure_bucket_exists("mlflow")

    # 1. SETUP MLFLOW
    mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])
    mlflow.set_experiment("sasrec_strict_retrain")
    mlflow.tensorflow.autolog(log_models=True, log_datasets=False)

    with mlflow.start_run() as run:
        
        # 2. CHECK MODEL EXISTENCE (CRITICAL STEP)
        print(f"🔍 Checking for existing model at: {MODEL_SAVE_PATH}")
        if not os.path.exists(MODEL_SAVE_PATH):
            print(f"⛔ STOPPING: Không tìm thấy file model cũ tại {MODEL_SAVE_PATH}")
            print("⚠️ Config: 'Never create new'. Chương trình sẽ kết thúc ngay lập tức.")
            sys.exit(0) # <--- DỪNG CHƯƠNG TRÌNH

        # 3. LOAD DATA (Chỉ load khi biết chắc chắn có model)
        item_seqs, cat_seqs, last_times, vocab_size, num_categories = load_data()
        popular_items = get_popular_items(item_seqs)

        # 4. SPLIT DATA
        train_items, train_cats = [], []
        val_items, val_cats = [], []
        test_set = []

        print("✂️ Splitting Data...")
        for seq, cat, ts in zip(item_seqs, cat_seqs, last_times):
            if len(seq) < 3: continue 
            test_set.append({"input_items": seq[:-1], "input_cats": cat[:-1], "label": seq[-1], "test_time": ts})
            val_items.append(seq[:-1])
            val_cats.append(cat[:-1])
            train_items.append(seq[:-2])
            train_cats.append(cat[:-2])

        # 5. CREATE DATASETS
        train_ds = create_dataset(train_items, train_cats, MAX_LEN, vocab_size, popular_items, is_training=True)
        val_ds = create_dataset(val_items, val_cats, MAX_LEN, vocab_size, popular_items, is_training=False)

        # 6. LOAD MODEL (WARM START)
        try:
            print("🔄 Found model. Attempting to load for Fine-tuning...")
            # Load với compile=False để ta tự set lại Learning Rate
            model = tf.keras.models.load_model(
                MODEL_SAVE_PATH, 
                custom_objects={'SasRec': SasRec},
                compile=False 
            )
            print("✅ Model loaded successfully! Ready for fine-tuning.")
        except Exception as e:
            print(f"❌ CRITICAL ERROR: File model tồn tại nhưng bị lỗi.")
            print(f"   Details: {e}")
            sys.exit(1) # <--- DỪNG CHƯƠNG TRÌNH (Báo lỗi)

        # 7. COMPILE (FINE-TUNE LR)
        print(f"⚙️ Compiling with Fine-tuning LR: {FINE_TUNE_LR}")
        
        lr_schedule = tf.keras.optimizers.schedules.CosineDecayRestarts(
            initial_learning_rate=FINE_TUNE_LR,
            first_decay_steps=STEPS_PER_EPOCH * 5,
            t_mul=2.0, m_mul=0.9, alpha=1e-6
        )
        
        model.compile(optimizer=tf.keras.optimizers.AdamW(learning_rate=lr_schedule, weight_decay=0.01))

        # Log Params
        mlflow.log_params({
            "mode": "strict_retrain",
            "base_model": MODEL_SAVE_PATH,
            "learning_rate": FINE_TUNE_LR
        })

        # 8. START TRAINING
        print(f"🔥 Bắt đầu Retrain (Fine-tune)...")
        history = model.fit(
            train_ds,
            validation_data=val_ds,
            epochs=EPOCHS,
            steps_per_epoch=STEPS_PER_EPOCH,
            validation_steps=50,
            callbacks=[
                tf.keras.callbacks.EarlyStopping(monitor='val_loss', patience=3, restore_best_weights=True),
                # Lưu đè lên file cũ để cập nhật model
                tf.keras.callbacks.ModelCheckpoint(MODEL_SAVE_PATH, save_best_only=True, monitor='val_loss')
            ]
        )

        # 9. SAVE & UPLOAD
        print("💾 Saving artifacts...")
        with open(TEST_SET_SAVE_PATH, 'wb') as f:
            pickle.dump(test_set, f)
        
        # ModelCheckpoint đã lưu bản tốt nhất, nhưng ta lưu thêm lần cuối cho chắc
        model.save(MODEL_SAVE_PATH)

        try:
            # Upload model mới lên MLflow
            mlflow.log_artifact(TEST_SET_SAVE_PATH, artifact_path="data_splits")
            mlflow.log_artifact(MODEL_SAVE_PATH, artifact_path="model_keras")
            
            if os.path.exists(LOG_PATH):
                with open(LOG_PATH, 'w') as f: json.dump(history.history, f)
                mlflow.log_artifact(LOG_PATH, artifact_path="logs")
            
            print("✅ Strict Retrain Completed & Uploaded!")
        except Exception as e:
            print(f"⚠️ Upload Failed: {e}")

if __name__ == "__main__":
    main()