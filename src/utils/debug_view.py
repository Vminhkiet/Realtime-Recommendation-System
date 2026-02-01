import pickle
import os
from datetime import datetime
import statistics

# ĐƯỜNG DẪN
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(BASE_DIR))
TEST_PATH = os.path.join(PROJECT_ROOT, 'data/model_registry/test_set.pkl')

def main():
    print("🕵️ ĐANG QUÉT THỜI GIAN CỦA TEST SET...\n")

    if not os.path.exists(TEST_PATH):
        print("❌ Không tìm thấy file test_set.pkl")
        return

    with open(TEST_PATH, 'rb') as f:
        test_set = pickle.load(f)

    # Lấy danh sách timestamp
    # Lưu ý: test_time có thể là float hoặc int
    timestamps = []
    for sample in test_set:
        ts = sample.get('test_time')
        if ts:
            timestamps.append(ts)

    if not timestamps:
        print("⚠️ Không tìm thấy thông tin thời gian trong Test Set.")
        return

    # Thống kê
    min_ts = min(timestamps)
    max_ts = max(timestamps)
    avg_ts = statistics.mean(timestamps)

    print(f"📊 Tổng số mẫu Test: {len(timestamps)}")
    print("-" * 40)
    print(f"🕒 Test cũ nhất (Min) : {datetime.fromtimestamp(min_ts)}")
    print(f"🕒 Test mới nhất (Max) : {datetime.fromtimestamp(max_ts)}")
    print(f"🕒 Trung bình (Avg)    : {datetime.fromtimestamp(avg_ts)}")
    print("-" * 40)

    # Kiểm tra xem có sát ngày giả lập không
    print("\n💡 NHẬN XÉT CHO BÁO CÁO:")
    if datetime.fromtimestamp(max_ts).year == 2025:
        print("✅ Dữ liệu Test ĐÃ KHỚP với kịch bản năm 2025.")
        print(f"   Simulation của bạn sẽ chạy nối tiếp ngay sau ngày: {datetime.fromtimestamp(max_ts).strftime('%d/%m/%Y')}")
    else:
        print("⚠️ Dữ liệu Test chưa khớp năm 2025. Hãy kiểm tra lại bước Hack Time!")

if __name__ == "__main__":
    main()