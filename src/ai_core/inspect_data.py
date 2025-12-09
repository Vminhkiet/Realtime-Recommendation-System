import pickle
import os

# Đường dẫn file
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(BASE_DIR))
DATA_PATH = os.path.join(PROJECT_ROOT, 'data/model_registry/dataset.pkl')
MAP_PATH = os.path.join(PROJECT_ROOT, 'data/model_registry/item_map.pkl')

def inspect():
    print(f"🔍 Đang kiểm tra file: {DATA_PATH}")
    
    # 1. Đọc file dataset.pkl (Chứa chuỗi hành vi)
    try:
        with open(DATA_PATH, 'rb') as f:
            dataset = pickle.load(f)
            
        print("\n--- 1. THÔNG TIN DATASET ---")
        print(f"Kiểu dữ liệu: {type(dataset)}")
        print(f"Tổng số User/Session: {len(dataset)}")
        print(f"Cấu trúc mẫu số 1: {dataset[0]}")
        print(f"Độ dài mẫu số 1: {len(dataset[0])}")
        
    except Exception as e:
        print(f"❌ Lỗi đọc dataset: {e}")
        return

    # 2. Đọc file item_map.pkl (Chứa từ điển dịch mã)
    try:
        with open(MAP_PATH, 'rb') as f:
            # Lưu ý: Lúc save ta save tuple (item2id, id2item)
            # Nếu chỉ save 1 cái thì sửa dòng dưới thành: item2id = pickle.load(f)
            data = pickle.load(f)
            
            if isinstance(data, tuple):
                item2id, id2item = data
            else:
                item2id = data
                id2item = {v: k for k, v in item2id.items()} # Tạo ngược lại nếu thiếu

        print("\n--- 2. THÔNG TIN MAPPING ---")
        print(f"Tổng số sản phẩm: {len(item2id)}")
        print("5 sản phẩm đầu tiên (ID -> Tên):")
        
        # In thử 5 món đầu tiên
        count = 0
        for idx, name in id2item.items():
            print(f"  ID {idx}: {name}")
            count += 1
            if count >= 5: break
            
    except Exception as e:
        print(f"❌ Lỗi đọc map: {e}")
        return

    # 3. Dịch thử mẫu số 1 ra tên thật
    print("\n--- 3. DỊCH MẪU SỐ 1 (DECODE) ---")
    sample_seq = dataset[0]
    # Lọc bỏ số 0 (Padding)
    real_items = [i for i in sample_seq if i != 0]
    
    print(f"Input gốc (Số): {sample_seq}")
    print("Dịch ra tên Amazon:")
    for i in real_items:
        print(f"  -> {id2item.get(i, 'Unknown')}")

if __name__ == "__main__":
    inspect()