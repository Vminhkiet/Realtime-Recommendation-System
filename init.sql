-- Kết nối vào database ecommerce_logs
\c ecommerce_logs

-- 1. Tạo bảng Sự kiện
CREATE TABLE IF NOT EXISTS user_interactions (
    "timestamp"    TIMESTAMPTZ NOT NULL,
    user_id      TEXT NOT NULL,
    item_id      TEXT NOT NULL,
    item_idx     INT NOT NULL,
    event_type   TEXT,
    rating       FLOAT4
);
SELECT create_hypertable('user_interactions', 'timestamp', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS ix_user_time ON user_interactions (user_id, "timestamp" DESC);

-- 2. Tạo bảng Log Gợi ý
CREATE TABLE IF NOT EXISTS recommendation_logs (
    request_id        UUID DEFAULT gen_random_uuid(),
    "timestamp"       TIMESTAMPTZ DEFAULT NOW(),
    user_id           TEXT NOT NULL,
    recommended_items INT[],
    clicked_item      INT,
    model_version     TEXT,
    PRIMARY KEY (request_id, "timestamp")
);
SELECT create_hypertable('recommendation_logs', 'timestamp', if_not_exists => TRUE);

-- 3. Tạo Continuous Aggregate (Thống kê xu hướng)
CREATE MATERIALIZED VIEW IF NOT EXISTS top_trending_items
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('15 minutes', "timestamp") AS bucket,
    item_id,
    COUNT(*) as interaction_count
FROM user_interactions
GROUP BY bucket, item_id
WITH NO DATA;