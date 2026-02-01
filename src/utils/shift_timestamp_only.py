import json
import time
import os
from datetime import datetime

# ================= CẤU HÌNH =================
INPUT_FILE = '/home/minhk/project/Realtime-Recommendation-System/data/raw_source/Video_Games.jsonl'
OUTPUT_FILE = '/home/minhk/project/Realtime-Recommendation-System/data/raw_source/Video_Games_2025.jsonl'

def normalize_ts(ts):
    """
    Chuyển đổi Milliseconds sang Seconds nếu cần.
    Ngưỡng 32503680000 tương ứng với năm 3000. 
    Nếu ts lớn hơn số này -> chắc chắn là millisec -> chia 1000.
    """
    try:
        ts = float(ts)
        if ts > 32503680000: 
            return int(ts / 1000)
        return int(ts)
    except:
        return 0

def main():
    print(f"📖 Đang quét file: {INPUT_FILE} ...")
    
    # BƯỚC 1: TÌM MAX TIMESTAMP CŨ
    max_ts = 0
    count = 0
    
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                row = json.loads(line)
                # 🔥 FIX: Chuẩn hóa về giây trước khi so sánh
                ts = normalize_ts(row.get('timestamp', 0))
                
                if ts > max_ts:
                    max_ts = ts
                count += 1
            except:
                continue
    
    if max_ts == 0:
        print("❌ Lỗi: Không tìm thấy timestamp hợp lệ nào!")
        return

    print(f"📊 Đã quét {count} dòng.")
    print(f"📅 Data cũ kết thúc: {datetime.fromtimestamp(max_ts)}")

    # BƯỚC 2: TÍNH OFFSET (ĐỘ LỆCH)
    now_ts = int(time.time())
    offset = now_ts - max_ts - 86400 # Lùi 1 ngày
    
    print(f"🔄 Sẽ cộng thêm vào mỗi dòng: {offset} giây.")

    # BƯỚC 3: GHI FILE MỚI
    print(f"💾 Đang tạo file năm 2025: {OUTPUT_FILE} ...")
    
    with open(INPUT_FILE, 'r', encoding='utf-8') as f_in, \
         open(OUTPUT_FILE, 'w', encoding='utf-8') as f_out:
        
        for line in f_in:
            try:
                row = json.loads(line)
                
                # 🔥 FIX: Chuẩn hóa timestamp gốc về giây trước khi cộng
                original_ts = normalize_ts(row.get('timestamp', 0))
                
                if original_ts == 0: continue

                # Cộng thời gian
                row['timestamp'] = original_ts + offset
                
                # Ghi lại vào file mới
                f_out.write(json.dumps(row) + '\n')
            except:
                continue

    print("🎉 XONG! Hãy dùng file 'Video_Games_2025.jsonl' cho các bước tiếp theo.")
    print(f"✅ Data mới kết thúc: {datetime.fromtimestamp(max_ts + offset)}")

if __name__ == "__main__":
    main()