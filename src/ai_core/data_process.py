import json
import pickle
import pandas as pd
import os

# --- CẤU HÌNH ĐƯỜNG DẪN TUYỆT ĐỐI ---
# Giúp chạy được từ bất kỳ đâu mà không lỗi Path
BASE_DIR = os.path.dirname(os.path.abspath(__file__)) # Thư mục src/ai_core
PROJECT_ROOT = os.path.dirname(os.path.dirname(BASE_DIR)) # Thư mục gốc project

RAW_DATA = os.path.join(PROJECT_ROOT, 'data/raw_source/All_Beauty.jsonl')
OUTPUT_DIR = os.path.join(PROJECT_ROOT, 'data/model_registry/')

def process():
    print(f"📂 Đang đọc dữ liệu từ: {RAW_DATA}")
    
    if not os.path.exists(RAW_DATA):
        print(f"❌ LỖI: Không tìm thấy file tại {RAW_DATA}")
        print("💡 Hãy tải file Amazon về và đổi tên thành All_Beauty.jsonl")
        return

    data = []
    # 1. ĐỌC FILE VÀ LỌC DỮ LIỆU
    try:
        with open(RAW_DATA, 'r') as f:
            for line in f:
                try:
                    row = json.loads(line)
                    
                    # Lấy rating, nếu không có mặc định là 0
                    rating = float(row.get('rating') or row.get('overall') or 0.0)
                    
                    # Lọc: Chỉ lấy hành vi tích cực (Rating >= 3)
                    # Hoặc nếu là dữ liệu click giả lập thì lấy hết
                    if rating >= 3.0: 
                        data.append({
                            'user_id': row.get('user_id') or row.get('reviewerID'),
                            'item_id': row.get('asin') or row.get('parent_asin'),
                            'time': row.get('timestamp') or row.get('unixReviewTime')
                        })
                except: continue
    except Exception as e:
        print(f"❌ Lỗi đọc file: {e}")
        return

    # Chuyển sang DataFrame để xử lý nhanh hơn
    df = pd.DataFrame(data)
    # Loại bỏ các dòng bị Null
    df = df.dropna()
    
    print(f"✅ Đã đọc {len(df)} dòng tương tác hợp lệ.")

    # 2. TẠO TỪ ĐIỂN MAPPING (Quan trọng nhất)
    # AI chỉ hiểu số, không hiểu chữ. Ta phải đánh số cho từng món hàng.
    print("🔄 Đang đánh số sản phẩm (Indexing)...")
    
    item_list = df['item_id'].unique()
    
    # item2id: B00YQ... -> 1
    # Số 0 dành riêng cho padding (đệm), nên bắt đầu từ 1
    item2id = {item: i+1 for i, item in enumerate(item_list)}
    
    # id2item: 1 -> B00YQ... (Dùng để dịch ngược khi hiển thị)
    id2item = {i+1: item for i, item in enumerate(item_list)}
    
    # 3. TẠO CHUỖI HÀNH VI (User Sequence)
    print("🔄 Đang gom nhóm hành vi theo User...")
    
    # Sắp xếp theo thời gian để đảm bảo thứ tự Quá khứ -> Tương lai
    df_sorted = df.sort_values('time')
    user_groups = df_sorted.groupby('user_id')
    
    sequences = []
    for user_id, group in user_groups:
        # Chuyển toàn bộ Item ID của user đó sang số
        item_ids = [item2id[x] for x in group['item_id'].values]
        
        # Chỉ lấy những user có ít nhất 2 hành động (để có cái mà đoán)
        if len(item_ids) >= 2:
            sequences.append(item_ids)

    # 4. LƯU KẾT QUẢ
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Lưu danh sách chuỗi để train
    with open(os.path.join(OUTPUT_DIR, 'dataset.pkl'), 'wb') as f:
        pickle.dump(sequences, f)
        
    # Lưu từ điển để dịch ngược sau này
    with open(os.path.join(OUTPUT_DIR, 'item_map.pkl'), 'wb') as f:
        pickle.dump((item2id, id2item), f)
        
    print("-" * 30)
    print(f"✅ HOÀN TẤT XỬ LÝ!")
    print(f"📊 Tổng số User (Sequences): {len(sequences)}")
    print(f"📊 Tổng số Sản phẩm (Vocab): {len(item2id)}")
    print(f"💾 File lưu tại: {OUTPUT_DIR}")

if __name__ == "__main__":
    process()