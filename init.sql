-- Kết nối vào database ecommerce_logs
\c ecommerce_logs

-- ==========================================================
-- 1. BẢNG SỰ KIỆN TƯƠNG TÁC (Processed User Actions)
-- Lưu trữ data từ Kafka Sink (Spark -> Kafka -> TimescaleDB)
-- ==========================================================
CREATE TABLE IF NOT EXISTS user_interactions (
    "timestamp"     TIMESTAMPTZ NOT NULL,
    user_id         TEXT NOT NULL,
    item_id         TEXT NOT NULL,
    item_idx        INT NOT NULL,
    event_type      TEXT,
    rating          FLOAT4
);

-- Biến bảng thành Hypertable để tối ưu dữ liệu chuỗi thời gian
SELECT create_hypertable('user_interactions', 'timestamp', if_not_exists => TRUE);

-- Index để truy vấn lịch sử gần đây của User cực nhanh (phục vụ Inference)
CREATE INDEX IF NOT EXISTS ix_user_time ON user_interactions (user_id, "timestamp" DESC);

-- Chính sách nén dữ liệu (Compression): Giảm 90% dung lượng đĩa
ALTER TABLE user_interactions SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'user_id'
);
SELECT add_compression_policy('user_interactions', INTERVAL '7 days');

-- Chính sách xóa dữ liệu cũ (Retention): Giữ lại 1 năm để re-train model
SELECT add_retention_policy('user_interactions', INTERVAL '1 year');


-- ==========================================================
-- 2. BẢNG THỐNG KÊ XU HƯỚNG (Continuous Aggregate)
-- Tự động tính toán Top Trending Items mỗi 15 phút
-- ==========================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS top_trending_items
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('15 minutes', "timestamp") AS bucket,
    item_id,
    COUNT(*) as interaction_count,
    AVG(rating) as avg_rating
FROM user_interactions
GROUP BY bucket, item_id
WITH NO DATA;

-- Chính sách tự động cập nhật View Trending (Refresh Policy)
SELECT add_continuous_aggregate_policy('top_trending_items',
    start_offset => INTERVAL '2 hours',
    end_offset => INTERVAL '1 minute',
    schedule_interval => INTERVAL '5 minutes');


-- ==========================================================
-- 3. BẢNG QUẢN LÝ MÔ HÌNH (Model Registry Metadata)
-- API sẽ đọc bảng này để biết lấy model nào từ MinIO
-- ==========================================================
CREATE TABLE IF NOT EXISTS model_registry (
    model_id        TEXT PRIMARY KEY,
    version         INT NOT NULL,
    status          TEXT DEFAULT 'staging', -- 'production', 'staging', 'archived'
    model_path      TEXT NOT NULL,          -- Path trong MinIO (e.g. 'sasrec/v1/model.keras')
    mapping_path    TEXT NOT NULL,          -- Path file item_map.json trong MinIO
    metrics         JSONB,                  -- Lưu Precision, Recall, F1-score
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Index để API tìm nhanh model đang chạy thực tế
CREATE INDEX IF NOT EXISTS ix_model_active ON model_registry (status) WHERE status = 'production';


-- ==========================================================
-- 4. BẢNG LOG GỢI Ý (Recommendation Feedback)
-- Lưu lại kết quả AI đã gợi ý và phản hồi của người dùng
-- ==========================================================
CREATE TABLE IF NOT EXISTS recommendation_logs (
    request_id      UUID DEFAULT gen_random_uuid(),
    "timestamp"     TIMESTAMPTZ DEFAULT NOW(),
    user_id         TEXT NOT NULL,
    recommended_items INT[],   -- Danh sách item_idx đã gợi ý
    clicked_item    INT,       -- Sẽ update khi user click
    model_version   TEXT,
    PRIMARY KEY (request_id, "timestamp")
);

-- Biến bảng log thành Hypertable
SELECT create_hypertable('recommendation_logs', 'timestamp', if_not_exists => TRUE);

-- Chính sách xóa log cũ sau 6 tháng (Retention)
SELECT add_retention_policy('recommendation_logs', INTERVAL '6 months');

-- ==========================================================
-- DATA MẪU CHO MODEL REGISTRY (Giả lập dự án thật)
-- ==========================================================
-- Đảm bảo bảng đã tồn tại
CREATE TABLE IF NOT EXISTS model_registry (
    model_id        TEXT PRIMARY KEY,
    version         INT NOT NULL,
    status          TEXT DEFAULT 'staging',
    model_path      TEXT NOT NULL,
    mapping_path    TEXT NOT NULL,
    metrics         JSONB,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Chèn hoặc Cập nhật thông tin model trỏ về thư mục local của bạn
INSERT INTO model_registry (
    model_id, 
    version, 
    status, 
    model_path, 
    mapping_path
) VALUES (
    'sasrec-video-games', 
    1, 
    'production', 
    '/home/minhk/project/Realtime-Recommendation-System/data/model_registry/sasrec_v1.keras', 
    '/home/minhk/project/Realtime-Recommendation-System/data/model_registry/item_map.json'
) 
ON CONFLICT (model_id) 
DO UPDATE SET 
    model_path = EXCLUDED.model_path,
    mapping_path = EXCLUDED.mapping_path,
    status = 'production';

-- Kiểm tra lại kết quả
SELECT * FROM model_registry;