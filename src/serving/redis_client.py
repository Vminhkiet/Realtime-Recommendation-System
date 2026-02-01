import redis
import os
import json

class RedisFeatureStore:
    def __init__(self):
        # Kết nối tới service redis được định nghĩa trong docker-compose
        self.redis_host = os.getenv('REDIS_HOST', 'redis')
        self.redis_port = int(os.getenv('REDIS_PORT', 6379))
        self.client = redis.Redis(
            host=self.redis_host, 
            port=self.redis_port, 
            db=0, 
            decode_responses=True
        )
        print(f"📡 Connected to Redis Feature Store at {self.redis_host}:{self.redis_port}")

    def update_user_history(self, user_id, item_idx, max_len=50):
        """
        Thêm một click mới vào chuỗi lịch sử của User.
        Sử dụng cấu trúc dữ liệu List của Redis để đảm bảo thứ tự.
        """
        key = f"user_history:{user_id}"
        # Đẩy item mới vào đầu danh sách (Left Push)
        self.client.lpush(key, item_idx)
        # Giữ lại đúng MAX_LEN phần tử gần nhất để tối ưu RAM (Slide 1)
        self.client.ltrim(key, 0, max_len - 1)

    def get_user_history(self, user_id, max_len=50):
        """
        Reach back: Truy xuất ngược lịch sử click để đưa vào Model SASRec.
        """
        key = f"user_history:{user_id}"
        # Lấy toàn bộ danh sách hiện có
        history = self.client.lrange(key, 0, max_len - 1)
        
        # Chuyển đổi về dạng list integer
        history_indices = [int(idx) for idx in history]
        
        # Đảo ngược lại để đúng thứ tự thời gian (từ cũ đến mới)
        history_indices.reverse()
        return history_indices

    def check_health(self):
        try:
            return self.client.ping()
        except Exception:
            return False